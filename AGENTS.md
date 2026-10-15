<!-- ack-api-quirks:start -->
# dynamodb-controller - notes for AI agents

Observed AWS API behaviors ("quirks") for this service are documented in `docs/api-quirks/`. Read
`docs/api-quirks/README.md` first: it has a load guide (which file to read for which task) and per-resource documents with
each quirk's ACK implication and current handling status. The documents are generated from the ack-api-quirks lab
(https://github.com/aws-controllers-k8s/ack-api-quirks, `services/dynamodb/`), where the evidence for every finding lives; hand-written additions belong in the preserved
blocks of each document or in `docs/api-quirks/supplementary/`.

Development guidance for ACK controllers (code generation, hooks, testing) is in the ack-dev-skills repository.
<!-- ack-api-quirks:end -->
