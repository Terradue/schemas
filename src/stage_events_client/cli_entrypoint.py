# Copyright 2026 Terradue
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Dependency-light entry point for the optional command-line interface."""


def main() -> None:
    """Run the CLI, or explain how to install its optional dependencies."""
    try:
        from .cli import main as cli_main
    except ModuleNotFoundError as exc:
        if exc.name != "click":
            raise
        raise SystemExit(
            "The Stage Events CLI is not installed. "
            "Install it with: pip install 'stage-events-client[cli]'"
        ) from exc

    cli_main()


__all__ = ["main"]
