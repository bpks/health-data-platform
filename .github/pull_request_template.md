## What and why

<!-- One or two sentences. Link the ticket or incident if there is one. -->

## Type of change

- [ ] Pipeline code (src/)
- [ ] Serving layer SQL (sql/)
- [ ] Infrastructure (infra/)
- [ ] Monitoring or alerts (monitoring/)
- [ ] Docs only

## Risk and rollback

- Blast radius if this is wrong:
- How to roll back (revert commit, re-run a previous plan, restore from soft delete or point-in-time):
- Needs a change window or approval? (production infrastructure always does)

## Checks

- [ ] `make check` passes locally
- [ ] Terraform plan reviewed (for infra changes): nothing is destroyed that I did not intend
- [ ] No secrets, real data or personal data added (test data is synthetic)
- [ ] Runbook or docs updated if behaviour changed
