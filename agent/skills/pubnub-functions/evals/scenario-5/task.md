# API Rate Limiter

## Context

A messaging platform needs to prevent abuse by limiting how frequently individual users can publish messages. Each message includes `request.message.userId` identifying the sender. The function runs as a Before Publish handler on `api.*` channels.

Requirements:
- Each user is allowed a maximum of 50 messages per hour
- Users who exceed the limit should have their messages blocked
- The limit should reset automatically each hour without manual cleanup
- The system must handle concurrent requests from the same user correctly

## Task

Write a PubNub Before Publish function that enforces per-user rate limiting with automatic hourly reset. The implementation must work correctly in PubNub's serverless edge environment where there is no persistent in-memory state between executions.

## Expected Outputs

A single JavaScript file containing the complete PubNub Function implementation.
