# Collaboration Phase 1 API 使用说明（SQLite 本地版）

本文档对应当前后端新增的 Phase 1 协作接口（基于本地 SQLite）。

适用服务：`backend/main.py`  
数据库文件：`data/collab.sqlite3`（测试环境为 `data/collab_test.sqlite3`）

---

## 1. 快速流程

1. 注册用户：`POST /auth/register`
2. 登录获取 token：`POST /auth/login`
3. 携带 `Authorization: Bearer <token>` 访问项目接口
4. 创建项目：`POST /projects`
5. 获取项目状态：`GET /projects/{projectId}/state`
6. 提交状态（含版本号）：`POST /projects/{projectId}/state/commit`
7. 拉取事件：`GET /projects/{projectId}/events?fromVersion=...`

---

## 2. 认证接口

### 2.1 注册

`POST /auth/register`

请求体：

```json
{
  "email": "owner@example.com",
  "password": "Passw0rd!",
  "displayName": "Owner"
}
```

响应：

```json
{
  "user": {
    "id": "usr_xxx",
    "email": "owner@example.com",
    "displayName": "Owner",
    "createdAt": 1710000000000
  }
}
```

### 2.2 登录

`POST /auth/login`

请求体：

```json
{
  "email": "owner@example.com",
  "password": "Passw0rd!"
}
```

响应：

```json
{
  "accessToken": "<token>",
  "tokenType": "Bearer",
  "expiresAt": 1710000000000,
  "user": {
    "id": "usr_xxx",
    "email": "owner@example.com",
    "displayName": "Owner",
    "createdAt": 1710000000000
  }
}
```

### 2.3 当前用户

`GET /auth/me`

Header:

`Authorization: Bearer <accessToken>`

### 2.4 登出

`POST /auth/logout`

Header:

`Authorization: Bearer <accessToken>`

---

## 3. 项目接口

### 3.1 创建项目

`POST /projects`

请求体：

```json
{
  "name": "Team Project",
  "description": "phase1"
}
```

响应中包含当前用户在项目里的角色（创建者为 `owner`）。

### 3.2 项目列表

`GET /projects`

返回当前用户可见项目（成员关系过滤）。

### 3.3 项目详情

`GET /projects/{projectId}`

---

## 4. 项目成员接口

### 4.1 列出成员

`GET /projects/{projectId}/members`

### 4.2 添加/更新成员

`POST /projects/{projectId}/members`

请求体：

```json
{
  "memberEmail": "editor@example.com",
  "role": "editor"
}
```

说明：

1. 只允许 `owner/admin` 操作。
2. 同一成员重复添加会变为“更新角色”。
3. `role` 支持：`owner`、`admin`、`editor`、`viewer`。

### 4.3 修改指定成员角色

`PATCH /projects/{projectId}/members/{memberUserId}`

请求体：

```json
{
  "role": "viewer"
}
```

---

## 5. 状态与版本化提交

### 5.1 获取当前项目状态

`GET /projects/{projectId}/state`

响应：

```json
{
  "projectId": "prj_xxx",
  "version": 2,
  "state": {
    "tree": {
      "id": "root"
    }
  },
  "updatedBy": "usr_xxx",
  "updatedAt": 1710000000000
}
```

### 5.2 提交状态（核心）

`POST /projects/{projectId}/state/commit`

必须携带 `baseVersion`。后端会做并发检查：

1. `baseVersion == 当前版本`：提交成功，版本 +1
2. 不相等：返回 `409 conflict`

#### 方式A：整状态提交

```json
{
  "baseVersion": 2,
  "clientOpId": "op_001",
  "state": {
    "tree": {
      "id": "root",
      "name": "Project"
    }
  }
}
```

#### 方式B：patch 提交

```json
{
  "baseVersion": 2,
  "clientOpId": "op_002",
  "patches": [
    {
      "op": "set_top_level",
      "key": "note",
      "value": "updated"
    }
  ]
}
```

当前支持的 patch：

1. `replace_state`：用 `state` 完全替换
2. `set_top_level`：修改顶层键值

#### 幂等语义

同一个 `clientOpId` 重复提交时，不会重复写版本，会返回已有结果（`idempotent=true`）。

### 5.3 事件回放

`GET /projects/{projectId}/events?fromVersion=0&limit=100`

用于客户端断线重连后的版本追赶。

---

## 6. 权限规则

### 6.1 读权限

`owner/admin/editor/viewer` 均可：

1. 查看项目
2. 获取项目状态
3. 拉取事件

### 6.2 写权限

`owner/admin/editor` 可：

1. 提交项目状态

### 6.3 成员管理权限

仅 `owner/admin` 可：

1. 添加成员
2. 修改成员角色

---

## 7. 错误与状态码

1. `400`：参数错误（例如 role 非法、name 空）
2. `401`：未登录或 token 无效
3. `403`：权限不足
4. `404`：项目不存在或不可见
5. `409`：版本冲突（提交时 baseVersion 落后）

冲突示例（`409`）：

```json
{
  "detail": {
    "projectId": "prj_xxx",
    "conflict": true,
    "latestVersion": 3,
    "state": {
      "tree": {
        "id": "root"
      }
    }
  }
}
```

---

## 8. cURL 示例（可直接跑）

假设服务地址为 `http://localhost:8000`。

### 8.1 注册 + 登录

```bash
curl -s -X POST http://localhost:8000/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"email":"owner@example.com","password":"Passw0rd!","displayName":"Owner"}'

TOKEN=$(curl -s -X POST http://localhost:8000/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"owner@example.com","password":"Passw0rd!"}' | jq -r '.accessToken')
```

### 8.2 创建项目

```bash
PROJECT_ID=$(curl -s -X POST http://localhost:8000/projects \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"name":"Team Project","description":"phase1"}' | jq -r '.id')
```

### 8.3 获取状态并提交

```bash
curl -s http://localhost:8000/projects/$PROJECT_ID/state \
  -H "Authorization: Bearer $TOKEN"

curl -s -X POST http://localhost:8000/projects/$PROJECT_ID/state/commit \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"baseVersion":0,"state":{"tree":{"id":"root","name":"Project"}},"clientOpId":"op_1"}'
```

### 8.4 拉取事件

```bash
curl -s "http://localhost:8000/projects/$PROJECT_ID/events?fromVersion=0&limit=50" \
  -H "Authorization: Bearer $TOKEN"
```

---

## 9. 与旧接口关系

1. 旧 `/sessions/*` 接口仍保留，现有功能不受影响。
2. 新协作能力统一走 `/auth/*` 与 `/projects/*`。
3. 前端切换时建议优先改造：
   1. 登录态管理
   2. 项目列表与项目选择
   3. `baseVersion` 提交与 `409` 冲突处理
