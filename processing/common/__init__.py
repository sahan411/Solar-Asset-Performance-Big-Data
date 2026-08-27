"""Shared helpers for the SolarIQ processing subsystem (Member 2).

Contains only what the streaming job, the batch jobs and the storage tooling all
need: configuration, structured logging and PostgreSQL access. Business logic
lives in processing.streaming and processing.batch, never here.
"""
