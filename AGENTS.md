<!-- aws-api-semantics:start -->
# dynamodb-controller - notes for AI agents

Observed AWS API semantics for this service are documented in `docs/api-semantics/`. Read
`docs/api-semantics/README.md` first: it has a load guide (which file to read for which task) and per-resource documents with
each finding's ACK implication and current handling status. The documents are generated from the aws-api-semantics lab
(https://github.com/aws-controllers-k8s/aws-api-semantics, `services/dynamodb/`), where the evidence for every finding lives; hand-written additions belong in the preserved
blocks of each document or in `docs/api-semantics/supplementary/`.

Development guidance for ACK controllers (code generation, hooks, testing) is in the ack-dev-skills repository.
<!-- aws-api-semantics:end -->
