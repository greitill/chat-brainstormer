# Daily Stats Broadcaster

## Context

A dashboard application needs periodic updates with aggregated statistics. Every 5 minutes, a scheduled function should collect the latest metrics from storage and broadcast a summary to connected dashboard clients.

The function should:
- Read the current values for keys `stats:signups`, `stats:orders`, and `stats:errors` from persistent storage
- Bundle them into a single summary message
- Broadcast the summary to the `dashboard.stats` channel for all connected dashboard clients

## Task

Write a PubNub scheduled function that runs on an interval to collect and broadcast aggregated statistics. Use the appropriate function type and signature for a timer-triggered function.

## Expected Outputs

A single JavaScript file containing the complete PubNub Function implementation.
