#!/usr/bin/env python3
"""Bootstrap initial owner and default organization for local dev/deploy.

Usage:
  python backend/scripts/bootstrap_collab_owner.py --email owner@example.com --password Passw0rd! --name Owner
"""

import argparse
import json
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from collab_storage import collab_storage  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--email", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--name", default="")
    parser.add_argument("--org-name", default="")
    parser.add_argument("--project-name", default="")
    args = parser.parse_args()

    user = collab_storage.register_user(args.email, args.password, args.name)
    org_name = args.org_name.strip() or f"{(args.name or args.email).strip()} Workspace"
    org = collab_storage.create_organization(user["id"], org_name)

    project = None
    if args.project_name.strip():
        project = collab_storage.create_project(user["id"], args.project_name.strip(), "", org["id"])

    print(
        json.dumps(
            {
                "user": user,
                "organization": org,
                "project": project,
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
