# Security

Report security issues privately to the Hasna maintainers. Do not open public
issues for suspected vulnerabilities.

Automation specs and queue rows must store secret references, not raw secrets.
Runtime execution should apply sandbox, approval, and audit policy before any
action is claimed.
