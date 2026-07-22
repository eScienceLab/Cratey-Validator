# RO-Crate Validation Service

[RO-Crates](https://www.researchobject.org/ro-crate/) are a lightweight approach to packaging research data and metadata.
They are designed to be easy to create and consume, and can be used to share research data in a way that is both human and machine-readable.

To ensure that RO-Crates are valid and can be consumed by others, we need to validate them against a set of standards.
The base RO-Crate standards are defined in the [RO-Crate Metadata Specification](https://www.researchobject.org/ro-crate/1.2/metadata.html),
and include requirements for the structure of the RO-Crate, the metadata it contains, and the files it includes.
On top of this, there are also community-specific extensions to the RO-Crate standards,
such as the [Five Safes RO-Crate extension](https://trefx.uk/5s-crate/), for working with sensitive data within Trusted Research Environments (TREs).

This validation process can be complex, requiring both structural and semantic checks to ensure that the RO-Crate is compliant with the relevant standards.
The [rocrate-validator](https://rocrate-validator.readthedocs.io/en/latest/) provides a means for the structural validation of RO-Crates,
checking that they conform to the required structure and contain the necessary metadata.
These checks are carried out using a combination of [SHACL](https://www.w3.org/TR/shacl/) shapes, a language for validating RDF graphs against a set of constraints, and python functions.
It is available on [PyPI](https://pypi.org/project/roc-validator/) and can be installed using pip.

This RO-Crate Validation Service provides a REST API for validating RO-Crates using the rocrate-validator.
It is built in python using flask, and is provided as a docker image for ease of deployment.
Several base profiles are included, while more can be added as needed when the service is deployed.
The service accepts RO-Crates as zip files, and returns a validation report in JSON format, detailing any issues found with the RO-Crate.
