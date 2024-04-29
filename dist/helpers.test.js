"use strict";
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2023-Present The Kubernetes Fluent Client Authors
Object.defineProperty(exports, "__esModule", { value: true });
const globals_1 = require("@jest/globals");
const helpers_1 = require("./helpers");
(0, globals_1.describe)("helpers", () => {
    (0, globals_1.test)("fromEnv for NodeJS", () => {
        (0, globals_1.expect)(() => {
            (0, helpers_1.fromEnv)("MY_MISSING_ENV_VAR");
        }).toThrowError("Environment variable MY_MISSING_ENV_VAR is not set");
        process.env.MY_ENV_VAR = "my-value";
        (0, globals_1.expect)((0, helpers_1.fromEnv)("MY_ENV_VAR")).toEqual("my-value");
        delete process.env.MY_ENV_VAR;
    });
});
(0, globals_1.describe)("Cluster Wait Function", () => {
    (0, globals_1.it)("should resolve if the cluster is already ready", async () => {
        const cluster = await (0, helpers_1.waitForCluster)(5);
        (0, globals_1.expect)(cluster).toEqual({ server: "http://jest-test:8080" });
    });
});
