import assert from "node:assert/strict";
import test from "node:test";
import { parseServerInfoResponse, parseAppPolicyV1, parseDeploymentSwitchResponse, parseDeploymentSwitchPayload, parseApiResponse } from "../../clients/javascript/index.js";

const info = {
    app_name: "test", app_env_prefix: "TEST_", obelisk_version: "0.43.0",
    deployment_id: "Dep_example", node_run_id: "Run_example", app_config_digest: "digest",
    started_at: "2026-10-25T12:00:00Z",
};

test("info rejects missing fields and invalid timestamps, accepts additions", () => {
    for (const key of Object.keys(info)) {
        const value = { ...info };
        delete value[key];
        assert.throws(() => parseServerInfoResponse(value), TypeError);
    }
    for (const started_at of [null, 42, "invalid", "2026-02-30T12:00:00Z"]) {
        assert.throws(() => parseServerInfoResponse({ ...info, started_at }), TypeError);
    }
    const extended = { ...info, future: true };
    assert.equal(parseApiResponse("GET", "/v1/info", extended), extended);
});

test("switch rejects unknown outcomes and null flags without mutating defaults", () => {
    for (const ok of ["switched", "restart_required"]) {
        assert.deepEqual(parseApiResponse("PUT", "/v1/deployments/Dep_1/switch", { ok }), { ok });
    }
    for (const value of [{}, { ok: "unknown" }, { ok: 1 }, { err: "failed" }]) {
        assert.throws(() => parseDeploymentSwitchResponse(value), TypeError);
    }
    assert.deepEqual(parseDeploymentSwitchPayload({}), {});
    assert.throws(() => parseDeploymentSwitchPayload({ apply: null }), TypeError);
});

test("policy preserves nullable fields and rejects malformed unions", () => {
    const policy = {
        format: "obelisk-app-config-v1", secrets: {}, public_env: [], allowed_exec_activities: {},
        outbound_http: [{ pattern: "example.com", methods: "*", request_url_regex: null, secrets: [], replace_in: [] }],
    };
    assert.equal(parseAppPolicyV1(policy), policy);
    assert.throws(() => parseAppPolicyV1({ ...policy, extra: true }), TypeError);
    assert.throws(() => parseAppPolicyV1({ ...policy, public_env: [{ name: "ENV", type: "unknown" }] }), TypeError);
    assert.throws(() => parseAppPolicyV1({ ...policy, outbound_http: [{ ...policy.outbound_http[0], methods: "GET" }] }), TypeError);
});
