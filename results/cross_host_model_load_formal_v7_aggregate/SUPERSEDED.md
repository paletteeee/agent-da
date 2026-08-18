# Superseded cross-host evidence

The v7 aggregate and per-repetition summaries are retained only as pre-fix
audit history. A final security review found that the runner did not yet prove
that the SSH tunnel process owned the model endpoint listener throughout each
run. Therefore, v7 must not be cited as the final attested cross-host result.

The final claim-bearing evidence is the v8 three-repetition aggregate in
`results/cross_host_model_load_formal_v8_aggregate/`, produced after adding
preflight/final listener ownership checks, ControlMaster continuity checks,
strict forwarding validation, UTC offset validation, and the exact 3 x 600
second formal aggregation requirement.
