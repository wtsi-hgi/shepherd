"""
Copyright (c) 2019 Genome Research Limited

Author: Christopher Harrison <ch12@sanger.ac.uk>

This program is free software: you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the
Free Software Foundation, either version 3 of the License, or (at your
option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License along
with this program. If not, see https://www.gnu.org/licenses/
"""

from ..types import RouteScriptTransformation
from ..templating import wrapper_script


# Convenience wrapper
def _wrapper(script:str, shebang:str = "/usr/bin/env bash") -> RouteScriptTransformation:
    return RouteScriptTransformation(wrapper_script(f"#!{shebang}\n{script}"))


telemetry = _wrapper(r"""
declare start="$(date +%s)"

cat >&2 <<-EOF
	#### START TELEMETRY ###################################################
	## Source: {{ source.filesystem }} {{ source.address | sh_escape }}
	## Target: {{ target.filesystem }} {{ target.address | sh_escape }}
	## Username: $(id -un) ($(id -u))
	## Hostname: $(hostname)
	## Start Time: $(date -d "@${start}")
	## Environment:
	$(env | sed 's/^/## * /')
	#### START EXECUTION ###################################################
	EOF

# Run script in subshell
(
[[ script ]]
)

declare exit_status="$?"
declare finish="$(date +%s)"
declare runtime="$(( finish - start ))"

cat >&2 <<-EOF
	#### END EXECUTION #####################################################
	## Exit Status: ${exit_status}
	## Finish Time: $(date -d "@${finish}")
	## Run Time: ${runtime} seconds
	#### END TELEMETRY #####################################################
	EOF

# Thread exit status through
exit "${exit_status}"
""")


debugging = _wrapper(r"""
set -x

[[ script ]]
""")
