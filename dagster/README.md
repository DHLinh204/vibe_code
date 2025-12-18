# Dagster Project

This folder contains the Dagster data orchestration pipeline configuration.

## Project Structure

- **assets/**: Dagster asset definitions
- **jobs/**: Job definitions that orchestrate assets
- **resources/**: Resource definitions (databases, APIs, etc.)
- **sensors/**: Sensors for pipeline automation

## Getting Started

Install Dagster:

```bash
pip install dagster dagster-webui
```

Run Dagster UI:

```bash
cd dagster
dagster dev
```
