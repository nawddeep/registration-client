# Dependencies added for face liveness

| Component | Version | Licence | Used for |
|---|---|---|---|
| ONNX Runtime (`com.microsoft.onnxruntime:onnxruntime`) | 1.19.2 | MIT | runs the detector, landmark and PAD models |
| Gson | 2.10.1 | Apache-2.0 | config parsing and canonical JSON |
| JUnit 5 + vintage (tests only) | 5.6.2 | EPL-2.0 | core module tests |
| BlazeFace short-range (ONNX conversion) | pinned by SHA-256 in `liveness/desktop-models/manifest.json` | Apache-2.0 per its model card | face and eye detection |
| Face Mesh V2 (ONNX conversion) | pinned by SHA-256 in the same manifest | Apache-2.0 per its model card | facial landmarks |
| MiniFASNetV2 + MiniFASNetV1SE (ONNX) | see `registration-liveness-core/MODEL_CARD.md` | code Apache-2.0; **weights licence unclear** | passive presentation-attack detection |

Licence texts: `registration-services/src/main/resources/liveness/desktop-models/LICENSE-Apache-2.0.txt`.

Open item: the MiniFASNet weights have no clear OSI licence upstream. Until that is resolved, the rule that every
dependency is OSI-licensed is **not fully met**; replace the weights or obtain a written licence before release.
Sources and commits of the model conversions are recorded in `MODEL_CARD.md` and `UPSTREAM.md` in the core module.
