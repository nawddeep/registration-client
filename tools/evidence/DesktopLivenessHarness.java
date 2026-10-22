import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import io.mosip.registration.liveness.DesktopLivenessRuntime;
import io.mosip.registration.liveness.DesktopLivenessService;
import io.mosip.registration.liveness.SbiStreamTap;
import io.mosip.registration.livenessmanager.api.FaceCaptureDevice;
import io.mosip.registration.livenessmanager.api.LivenessEventListener;
import io.mosip.registration.livenessmanager.desktop.DesktopAuditManagerService;
import io.mosip.registration.livenessmanager.desktop.DesktopGlobalParamService;
import io.mosip.registration.livenessmanager.desktop.DesktopLivenessConfigAdapter;
import io.mosip.registration.livenessmanager.model.ChallengeType;
import io.mosip.registration.livenessmanager.model.LivenessEvent;
import io.mosip.registration.livenessmanager.model.LivenessState;
import io.mosip.registration.livenessmanager.model.Workflow;

/**
 * Evidence harness: feeds a local MJPEG stream (webcam through ffmpeg, or a recorded file) into the existing
 * {@code SbiStreamFrameSource} + {@code DesktopLivenessService} and prints the final state, the challenge types that
 * were shown and timings. TEST TOOL ONLY: it is not part of any module or jar.
 *
 * <p>
 * Every result from it is "Desktop engine via local webcam harness, not inside the running client".
 *
 * <p>
 * Output rules: no scores, no thresholds, no model names, no frames written anywhere. Audit descriptions from the
 * engine are discarded except the timing keys of PERFORMANCE records.
 *
 * <pre>
 *   java -cp ... DesktopLivenessHarness (--url http://127.0.0.1:8081/ | --file clip.mjpeg [--fps 15] [--loop])
 *                                       [--workflow RESIDENT|OPERATOR|SUPERVISOR] [--config signed.json]
 *                                       [--no-retry] [--timeout-seconds 120]
 * </pre>
 */
public final class DesktopLivenessHarness {

	private DesktopLivenessHarness() {
	}

	// ---- MJPEG parsing, inside the harness's own tap --------------------------------------------------------

	/**
	 * Reads a multipart MJPEG stream (ffmpeg {@code -f mpjpeg}, MockMDS style): boundary line, part headers, then the
	 * JPEG. Uses {@code Content-Length} when present, otherwise reads up to the next boundary.
	 */
	static final class MjpegReader {
		private final InputStream in;
		private byte[] boundary; // "--xxxx"

		MjpegReader(InputStream in) {
			this.in = new BufferedInputStream(in, 64 * 1024);
		}

		/** Next JPEG, or null at the end of the stream. */
		byte[] next() throws IOException {
			String line;
			// find a boundary line
			while (true) {
				line = readLine();
				if (line == null) {
					return null;
				}
				if (line.startsWith("--") && !line.equals("--")) {
					if (boundary == null) {
						boundary = line.trim().getBytes(StandardCharsets.ISO_8859_1);
					}
					break;
				}
			}
			int contentLength = -1;
			while (true) { // part headers up to the empty line
				line = readLine();
				if (line == null) {
					return null;
				}
				if (line.isEmpty()) {
					break;
				}
				int colon = line.indexOf(':');
				if (colon > 0 && line.substring(0, colon).trim().equalsIgnoreCase("Content-Length")) {
					contentLength = Integer.parseInt(line.substring(colon + 1).trim());
				}
			}
			if (contentLength > 0) {
				byte[] jpeg = new byte[contentLength];
				int off = 0;
				while (off < contentLength) {
					int n = in.read(jpeg, off, contentLength - off);
					if (n < 0) {
						return null;
					}
					off += n;
				}
				return jpeg;
			}
			return readUntilBoundary();
		}

		private byte[] readUntilBoundary() throws IOException {
			if (boundary == null) {
				return null;
			}
			ByteArrayOutputStream out = new ByteArrayOutputStream(64 * 1024);
			byte[] marker = ("\r\n" + new String(boundary, StandardCharsets.ISO_8859_1)).getBytes(StandardCharsets.ISO_8859_1);
			int matched = 0;
			int b;
			while ((b = in.read()) >= 0) {
				out.write(b);
				matched = (b == (marker[matched] & 0xff)) ? matched + 1 : (b == (marker[0] & 0xff) ? 1 : 0);
				if (matched == marker.length) {
					byte[] all = out.toByteArray();
					return java.util.Arrays.copyOf(all, all.length - marker.length);
				}
			}
			return null;
		}

		private String readLine() throws IOException {
			StringBuilder sb = new StringBuilder();
			int b;
			while ((b = in.read()) >= 0) {
				if (b == '\n') {
					int len = sb.length();
					if (len > 0 && sb.charAt(len - 1) == '\r') {
						sb.setLength(len - 1);
					}
					return sb.toString();
				}
				sb.append((char) b);
				if (sb.length() > 8192) { // not a text line: give up on this line
					return sb.toString();
				}
			}
			return sb.length() == 0 ? null : sb.toString();
		}
	}

	/** The tap: a daemon thread reads the MJPEG source and hands each JPEG to the frame source. */
	static final class MjpegTap implements SbiStreamTap {
		private final String url;
		private final File file;
		private final double fps;
		private final boolean loop;
		private final AtomicBoolean running = new AtomicBoolean();
		private volatile Thread pump;
		volatile long framesRead;

		MjpegTap(String url, File file, double fps, boolean loop) {
			this.url = url;
			this.file = file;
			this.fps = fps;
			this.loop = loop;
		}

		@Override
		public void attach(Consumer<byte[]> jpegListener) throws FaceCaptureDevice.DeviceException {
			try {
				openStream().close(); // fail fast, as a missing device would
			} catch (IOException e) {
				throw new FaceCaptureDevice.DeviceException("cannot open the MJPEG source", true, e);
			}
			running.set(true);
			pump = new Thread(() -> {
				try {
					do {
						try (InputStream raw = openStream()) {
							MjpegReader reader = new MjpegReader(raw);
							byte[] jpeg;
							while (running.get() && (jpeg = reader.next()) != null) {
								framesRead++;
								jpegListener.accept(jpeg);
								if (file != null) {
									Thread.sleep((long) (1000.0 / fps)); // a file is not paced by a camera
								}
							}
						}
					} while (running.get() && file != null && loop);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				} catch (IOException e) {
					// stream ended or broke: the frame source reports a stalled stream by itself
				}
			}, "HARNESS_MJPEG_PUMP");
			pump.setDaemon(true);
			pump.start();
		}

		@Override
		public void detach() {
			running.set(false);
			Thread t = pump;
			if (t != null) {
				t.interrupt();
			}
		}

		@Override
		public String deviceId() {
			return file != null ? "harness-mjpeg-file" : "harness-mjpeg-url";
		}

		private InputStream openStream() throws IOException {
			return file != null ? new FileInputStream(file) : new URL(url).openStream();
		}
	}

	// ---- run -------------------------------------------------------------------------------------------------

	public static void main(String[] args) throws Exception {
		String url = null;
		File file = null;
		double fps = 15;
		boolean loop = false;
		boolean retry = true;
		long timeoutSeconds = 120;
		Workflow workflow = Workflow.RESIDENT;
		String signedConfig = null;
		for (int i = 0; i < args.length; i++) {
			switch (args[i]) {
			case "--url":
				url = args[++i];
				break;
			case "--file":
				file = new File(args[++i]);
				break;
			case "--fps":
				fps = Double.parseDouble(args[++i]);
				break;
			case "--loop":
				loop = true;
				break;
			case "--no-retry":
				retry = false;
				break;
			case "--timeout-seconds":
				timeoutSeconds = Long.parseLong(args[++i]);
				break;
			case "--workflow":
				workflow = Workflow.valueOf(args[++i].toUpperCase());
				break;
			case "--config":
				signedConfig = new String(Files.readAllBytes(Paths.get(args[++i])), StandardCharsets.UTF_8).trim();
				break;
			default:
				System.err.println("unknown argument " + args[i]);
				System.exit(64);
			}
		}
		if ((url == null) == (file == null)) {
			System.err.println("give exactly one of --url or --file");
			System.exit(64);
		}

		// PERFORMANCE records carry timings only; everything else the engine audits is dropped here.
		List<String> perf = Collections.synchronizedList(new ArrayList<>());
		DesktopAuditManagerService audit = (event, moduleId, moduleName, description) -> {
			if (description != null && description.contains("type=PERFORMANCE")) {
				perf.add(description);
			}
		};
		final String configValue = signedConfig;
		DesktopGlobalParamService params = code -> DesktopLivenessConfigAdapter.GLOBAL_PARAM_KEY.equals(code)
				? configValue
				: null; // null = the last valid / bundled default config, exactly as the client falls back
		File dataDir = Files.createTempDirectory("liveness-harness").toFile();
		dataDir.deleteOnExit();
		DesktopLivenessRuntime runtime = new DesktopLivenessRuntime(params, audit, dataDir);
		runtime.refreshConfig(true);
		DesktopLivenessService service = new DesktopLivenessService(runtime);

		final long t0 = System.nanoTime();
		final Set<ChallengeType> challenges = new LinkedHashSet<>();
		final List<LivenessState> failures = Collections.synchronizedList(new ArrayList<>());
		final LivenessState[] last = new LivenessState[1];
		final LivenessState[] decision = new LivenessState[1]; // the state at the moment of decision, before cancel() adds IDLE
		final int[] attempts = { 1 };
		final long[] decisionMs = { -1 };
		final CountDownLatch done = new CountDownLatch(1);
		final boolean autoRetry = retry;

		LivenessEventListener listener = new LivenessEventListener() {
			@Override
			public void onEvent(LivenessEvent event) {
				LivenessState s = event.getState();
				last[0] = s;
				if (s == LivenessState.CHALLENGE && event.getChallengeType() != null) {
					challenges.add(event.getChallengeType());
				}
				LivenessState.Kind kind = s.getKind();
				if (kind == LivenessState.Kind.TERMINAL || kind == LivenessState.Kind.SYSTEM_ERROR) {
					decision[0] = s;
					decisionMs[0] = (System.nanoTime() - t0) / 1_000_000;
					done.countDown();
				} else if (kind == LivenessState.Kind.ATTEMPT_FAILURE) {
					failures.add(s);
					if (!autoRetry) {
						decision[0] = s;
						decisionMs[0] = (System.nanoTime() - t0) / 1_000_000;
						done.countDown();
					} else {
						// what the "Try again" button does; the service ignores it after the last attempt
						attempts[0]++;
						new Thread(service::retry, "HARNESS_RETRY").start();
					}
				}
			}
		};

		MjpegTap tap = new MjpegTap(url, file, fps, loop);
		service.start(workflow, tap, listener);
		boolean finished = done.await(timeoutSeconds, TimeUnit.SECONDS);
		long frames = tap.framesRead;
		service.cancel();
		service.shutdown();

		System.out.println("path: Desktop engine via local webcam harness, not inside the running client");
		System.out.println("source: " + (file != null ? "recorded file" : "live url"));
		System.out.println("workflow: " + workflow);
		System.out.println("final_state: " + (finished && decision[0] != null ? decision[0] : "NO_DECISION_WITHIN_" + timeoutSeconds + "s"));
		System.out.println("attempts: " + (failures.isEmpty() ? 1 : attempts[0]));
		System.out.println("attempt_failures: " + failures);
		System.out.println("challenges_shown: " + challenges);
		System.out.println("time_to_decision_ms: " + (decisionMs[0] >= 0 ? decisionMs[0] : -1));
		System.out.println("frames_read_from_source: " + frames);
		long framesAnalysed = 0;
		double weightedMean = 0;
		double worstP95 = 0;
		synchronized (perf) {
			for (String p : perf) {
				long n = longOf(p, "framesAnalysed");
				framesAnalysed += n;
				weightedMean += n * doubleOf(p, "meanMs");
				worstP95 = Math.max(worstP95, doubleOf(p, "p95Ms"));
			}
		}
		if (framesAnalysed > 0) {
			System.out.println(String.format(java.util.Locale.ROOT,
					"engine_timing: frames=%d mean_ms=%.1f worst_window_p95_ms=%.1f", framesAnalysed,
					weightedMean / framesAnalysed, worstP95));
		} else {
			System.out.println("engine_timing: no PERFORMANCE record");
		}
		System.exit(0);
	}

	private static String field(String description, String key) {
		for (String part : description.split(";")) {
			if (part.startsWith(key + "=")) {
				return part.substring(key.length() + 1);
			}
		}
		return null;
	}

	private static long longOf(String d, String key) {
		String v = field(d, key);
		return v == null ? 0 : Long.parseLong(v.trim());
	}

	private static double doubleOf(String d, String key) {
		String v = field(d, key);
		return v == null ? 0 : Double.parseDouble(v.trim());
	}
}
