# 中国空间站交会对接小游戏

面向《航天器轨道动力学》课程的单文件网页小游戏：学生通过设定两次脉冲机动参数，驾驶飞船完成与中国空间站的交会对接。轨道计算完全在浏览器内以确定性求解器完成（CW 闭式解 + 0.01 网格 + 分速度逐帧采样），教学数据可复核、可自证。

## 功能

- **交会对接模拟**：两次脉冲参数输入、实时轨迹可视化、Δv 放大镜、撞击判定、对接框入框判定
- **中英双语**：顶部地球图标一键切换，localStorage 持久化，首次访问按浏览器语言探测
- **AI 助教**：基于本地确定性求解器给出诊断与解释；支持流式对话（需配置模型 API），无 API 时自动回退规则模式；参数推荐卡仅在用户主动索要时给出
- **使用统计**：服务端权威统计（UV/PV/完成率/周趋势），支撑课程项目申报与"人工智能+虚拟仿真实验教学项目"数据自证
- **分享图**：任务成功后生成成绩分享图，便于学生传播
- **手机端适配**：响应式布局，二维码浮层方便扫码到手机体验

## 运行 / 部署

游戏本体是纯静态单文件，直接双击打开 `中国空间站交会对接小游戏.html` 即可离线游玩（统计与 AI 助教不可用）。

完整功能需启动 `server.py`（仅依赖 Python 3.8+ 标准库，无需 pip install）：

```bash
python3 server.py                       # http://127.0.0.1:8787/
python3 server.py --port 9000           # 自定义端口
python3 server.py --db /path/data.db    # 自定义 SQLite 库路径
python3 server.py --ai-provider zhipu   # AI 助教换用智谱（默认火山引擎 Ark）
```

用域名对外提供服务时（`ALLOWED_HOSTS` 必配）：

```bash
ALLOWED_HOSTS=game.example.com ARK_API_KEY=xxx ADMIN_TOKEN=xxx \
  python3 server.py --host 0.0.0.0 --port 8787
```

以上环境变量也可写入项目根目录 `.env`，启动时自动读取。

### 环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `ARK_API_KEY` | — | 火山引擎 Ark API Key；不设则 AI 助教进入规则兜底模式（仍可用） |
| `AI_PROVIDER` | `ark` | AI 供应商：`ark` / `zhipu`（命令行 `--ai-provider` 优先） |
| `ARK_MODEL` | `ark-code-latest` | Ark 主模型 |
| `ARK_MODEL_FALLBACK` | `deepseek-v4.1-flash` | 主模型空答/报错时自动重试的兜底模型 |
| `ARK_BASE_URL` | 火山官方地址 | 自定义 Ark 接入点 |
| `BIGMODEL_API_KEY` | — | 智谱 BigModel API Key（`--ai-provider zhipu` 时使用） |
| `BIGMODEL_MODEL` | `glm-5.3-flash` | 智谱模型 |
| `BIGMODEL_REASONING_EFFORT` | `low` | 智谱思考档位（glm-5.3 系列始终思考，low 避免 token 全耗在推理上） |
| `AI_MAX_TOKENS` | `3000` | 单次回答 token 上限（含 reasoning，给太小会空答） |
| `AI_RATE_PER_HOUR` | `20` | 单访客每小时 AI 请求上限 |
| `AI_MIN_GAP` | `1.2` | 单访客两次请求最小间隔（秒） |
| `AI_DAILY_LIMIT` | `800` | 全站每日 AI 请求上限（成本封顶） |
| `AI_IP_RATE_PER_HOUR` | `200` | 单 IP 每小时 AI 请求上限（防伪造 visitor_id 绕限流） |
| `AI_MAX_CONCURRENT` | `16` | AI 上游并发上限（超出者快速返回 503） |
| `EVENT_RATE_PER_MINUTE` | `300` | 单 IP 每分钟埋点请求上限（防刷库污染统计） |
| `ADMIN_TOKEN` | — | 设置后 `/api/events`、`/api/events.csv` 需 `?token=` 或 Bearer 鉴权 |
| `ALLOWED_HOSTS` | — | Host/Origin 域名白名单，逗号分隔；域名对外服务时必配，IP 与 localhost 始终放行 |
| `TERM_START` | 自动推断 | 本学期起始日（如 `2026-09-01`），不设则按 9/1、2/15 推断 |

更完整的口径与安全基线说明见 `server.py` 顶部注释。

## 项目结构

```
├── 中国空间站交会对接小游戏.html   # 游戏本体（单文件 HTML/CSS/JS，约 5400 行）
├── server.py                      # 本地服务：静态托管 + 埋点入库 + 统计快照 + AI 代理
└── 中国空间站交会对接小游戏_files/  # 游戏贴图资源
```

## License

[MIT](LICENSE) © 2026 祁瑞, 方辰
