# Python Data Movement Utility

This repository contains a portfolio example of a Python utility designed to move data between PostgreSQL RDS databases and Amazon S3 buckets, ensuring that all data at rest is encrypted. This code serves as a sample of DevOps-oriented utilities and is intended to demonstrate concepts rather than provide a fully functioning system.

## Overview

The utility is built around three core components:

1. **`MoveDataFactory`**
   - An abstract factory pattern implementation for creating specific data movement classes.
   - Provides a flexible way to extend functionality for additional data sources or destinations in the future.

2. **`ShippingAndReceiving`**
   - Responsible for performing the actual data movement operations between PostgreSQL and S3.
   - Ensures encryption and integrity of data during the transfer.

3. **`MyUtils`**
   - A collection of helper functions to handle repetitive tasks and streamline the implementation.
   - Includes utility functions for logging, configuration handling, and error management.

## Features

- **Secure Data Movement**: Data at rest is encrypted to comply with security best practices.
- **Modular Design**: Built with extensibility in mind to support additional environments or services in the future.
- **Environment-Specific**: Initially developed for a specific working environment but designed to be expanded into a generic utility.

## Requirements

- Python 3.10+
- Dependencies listed in `requirements.txt`
- Access to AWS S3 and PostgreSQL RDS instances

## Installation

1. Clone the repository:

   ```bash
   git clone <repository-url>
   cd <repository-folder>
   ```

2. Install required Python packages:

   ```bash
   pip install -r requirements.txt
   ```

3. Configure the environment variables required for your AWS and database credentials.

   ```bash
   TBD
   ```

## Usage

### Moving Data

TBD

Example:

```python
TBD
```

### Utility Functions

Use functions from `MyUtils` for tasks like logging and PostGRES queries.

Example:

```python
TBD
```

## Roadmap

- Generalize the utility to support additional databases and cloud storage services.
- Enhance error handling and logging for better debugging.
- Add support for data transformation during transfer.

## Contributing

TBD

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

---

*Note: This repository is a sample of utility code for demonstration purposes and is not intended for production use without modifications and testing.*
