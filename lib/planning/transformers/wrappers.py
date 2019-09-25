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

from ..transfer import RouteScriptTransformation
from ..templating import wrapper_script


_verbosity = wrapper_script(r"""#!/usr/bin/env bash

declare start="$(date +%s)"

cat >&2 <<-EOF
	## source: {{ source }}
	## target: {{ target }}
	## hostname: $(hostname)
	## start time: $(date -d "@${start}")
	EOF

# Run script in subshell
(
[[ script ]]
)

declare exit_status="$?"
declare finish="$(date +%s)"
declare runtime="$(( finish - start ))"

cat >&2 <<-EOF
	## exit status: ${exit_status}
	## finish time: $(date -d "@${finish}")
	## run time: ${runtime} seconds
	EOF
""")

verbosity = RouteScriptTransformation(_verbosity)
