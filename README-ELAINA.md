# elaina 分支说明

这个分支是在 [上游仓库](https://github.com/lifefuck/qq-deepseek-realistic-bot) 基础上做的**独立改造版**，
和 `main` 的差别很大，但**底座完全一致**（OneBot11 + NapCat + DeepSeek）。

## 两个分支怎么互抄

```
upstream/main   同学维护的原版
origin/elaina   这个分支（我这边的改造）

取他的新功能：
  git fetch upstream
  git log --oneline upstream/main ^HEAD        # 看他新增了什么
  git diff HEAD upstream/main -- bot.py        # 只看核心逻辑差异
  ⇒ 手动挑要的段落搬过来（两边结构不同，不建议直接 merge）

把我的功能给他：
  他加 remote 指向这个 fork，然后 fetch 我的分支，同样方式挑
```

## 这个分支相比原版做了什么

| 方向 | 内容 |
|---|---|
| 人格 | 换成伊蕾娜（4373 字），群/私聊两套语气，不叫"主人" |
| 安全 | 新增敏感词硬过滤（政治/违法/色情，102 词条，输入+输出双向拦截，群聊命中直接静默） |
| 图片 | 支持识图（走 DeepSeek 多模态，不需额外视觉模型） |
| 消息 | 合并转发展开、卡片消息解析、外链混淆、B站 API 取标题/UP主/播放量 |
| 群聊 | 冷热场动态权重、群友疲劳度、禁言实时检测、反模板闸、去句末句号、低信息量过滤 |
| 记忆 | 用户画像 `/memory`、长期印象注入 |
| 表情包 | 按情绪分类发送（目录留空即跳过） |
| 命令 | `/好感度` `/memory` `/暂停` `/恢复` `/new` `/compress` |
| 触发词 | 支持"仅主人触发"的词（如"老婆"只有管理员说才响应） |
| 部署 | systemd 两个 unit（Xvfb+QQ / bot.py），崩溃自动重启 |

## 部署

```bash
cp config.example.json config.json
cp whitelist.example.json whitelist.json
cp group_configs.example.json group_configs.json
# 填 config.json：owner_id、api.key、ws_token（要和 NapCat 的 onebot11 配置一致）
pip install websockets
python bot.py
```

NapCat 侧：QQ 端要注入 NapCat.Shell，`onebot11_<QQ号>.json` 里开 websocket 服务端指向 `127.0.0.1:3000`，token 与 `config.json` 的 `ws_token` 一致。

## 注意

- `config.json` / `whitelist.json` / `group_configs.json` / `affinity.json` / `tokens.json` 都**不进版本库**（含密钥、QQ 号、群号）
- 表情包图片不进库，只保留目录结构与说明
- `QQn/`（QQ 客户端副本）不进库
