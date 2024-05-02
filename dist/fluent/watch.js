"use strict";
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2023-Present The Kubernetes Fluent Client Authors
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.Watcher = exports.WatchEvent = void 0;
const byline_1 = __importDefault(require("byline"));
const crypto_1 = require("crypto");
const events_1 = require("events");
const node_fetch_1 = __importDefault(require("node-fetch"));
const types_1 = require("./types");
const utils_1 = require("./utils");
var WatchEvent;
(function (WatchEvent) {
    /** Watch is connected successfully */
    WatchEvent["CONNECT"] = "connect";
    /** Network error occurs */
    WatchEvent["NETWORK_ERROR"] = "network_error";
    /** Error decoding data or running the callback */
    WatchEvent["DATA_ERROR"] = "data_error";
    /** Reconnect is called */
    WatchEvent["RECONNECT"] = "reconnect";
    /** Retry limit is exceeded */
    WatchEvent["GIVE_UP"] = "give_up";
    /** Abort is called */
    WatchEvent["ABORT"] = "abort";
    /** Resync is called */
    WatchEvent["RESYNC"] = "resync";
    /** Data is received and decoded */
    WatchEvent["DATA"] = "data";
    /** Bookmark is received */
    WatchEvent["BOOKMARK"] = "bookmark";
    /** ResourceVersion is updated */
    WatchEvent["RESOURCE_VERSION"] = "resource_version";
    /** 410 (old resource version) occurs */
    WatchEvent["OLD_RESOURCE_VERSION"] = "old_resource_version";
    /** A reconnect is already pending */
    WatchEvent["RECONNECT_PENDING"] = "reconnect_pending";
})(WatchEvent || (exports.WatchEvent = WatchEvent = {}));
/** A wrapper around the Kubernetes watch API. */
class Watcher {
    // User-provided properties
    #model;
    #filters;
    #callback;
    #watchCfg;
    // Create a wrapped AbortController to allow the watch to be aborted externally
    #abortController;
    // Track the number of retries
    #retryCount = 0;
    // Create a stream to read the response body
    #stream;
    // Create an EventEmitter to emit events
    #events = new events_1.EventEmitter();
    // Create a timer to resync the watch
    #resyncTimer;
    // Track if a reconnect is pending
    #pendingReconnect = false;
    /**
     * Setup a Kubernetes watcher for the specified model and filters. The callback function will be called for each event received.
     * The watch can be aborted by calling {@link Watcher.close} or by calling abort() on the AbortController returned by {@link Watcher.start}.
     *
     *
     * Kubernetes API docs: {@link https://kubernetes.io/docs/reference/using-api/api-concepts/#efficient-detection-of-changes}
     *
     * @param model - the model to use for the API
     * @param filters - (optional) filter overrides, can also be chained
     * @param callback - the callback function to call when an event is received
     * @param watchCfg - (optional) watch configuration
     */
    constructor(model, filters, callback, watchCfg = {}) {
        // Set the retry delay to 10 seconds if not specified
        watchCfg.retryDelaySec ??= 10;
        // Set the resync interval to 5 minutes if not specified
        watchCfg.resyncIntervalSec ??= 300;
        // Bind class properties
        this.#model = model;
        this.#filters = filters;
        this.#callback = callback;
        this.#watchCfg = watchCfg;
        // Create a new AbortController
        this.#abortController = new AbortController();
    }
    /**
     * Start the watch.
     *
     * @returns The AbortController for the watch.
     */
    async start() {
        await this.#runner();
        return this.#abortController;
    }
    /** Close the watch. Also available on the AbortController returned by {@link Watcher.start}. */
    close() {
        clearTimeout(this.#resyncTimer);
        this.#cleanup();
        this.#abortController.abort();
    }
    /**
     * Get a unique ID for the watch based on the model and filters.
     * This is useful for caching the watch data or resource versions.
     *
     * @returns the watch CacheID
     */
    getCacheID() {
        // Build the URL, we don't care about the server URL or resourceVersion
        const url = (0, utils_1.pathBuilder)("https://ignore", this.#model, this.#filters, false);
        // Hash and truncate the ID to 10 characters, cache the result
        return (0, crypto_1.createHash)("sha224")
            .update(url.pathname + url.search)
            .digest("hex")
            .substring(0, 10);
    }
    /**
     * Get the current resource version.
     *
     * @returns the current resource version
     */
    get resourceVersion() {
        return this.#watchCfg.resourceVersion;
    }
    /**
     * Set the current resource version.
     *
     * @param resourceVersion - the new resource version
     */
    set resourceVersion(resourceVersion) {
        this.#watchCfg.resourceVersion = resourceVersion;
    }
    /**
     * Subscribe to watch events. This is an EventEmitter that emits the following events:
     *
     * Use {@link WatchEvent} for the event names.
     *
     * @returns an EventEmitter
     */
    get events() {
        return this.#events;
    }
    /**
     * Build the URL and request options for the watch.
     *
     * @returns the URL and request options
     */
    #buildURL = async () => {
        // Build the path and query params for the resource, excluding the name
        const { opts, serverUrl } = await (0, utils_1.k8sCfg)("GET");
        const url = (0, utils_1.pathBuilder)(serverUrl, this.#model, this.#filters, true);
        // Enable the watch query param
        url.searchParams.set("watch", "true");
        // If a name is specified, add it to the query params
        if (this.#filters.name) {
            url.searchParams.set("fieldSelector", `metadata.name=${this.#filters.name}`);
        }
        // If a resource version is specified, add it to the query params
        if (this.#watchCfg.resourceVersion) {
            url.searchParams.set("resourceVersion", this.#watchCfg.resourceVersion);
        }
        // Enable watch bookmarks
        // url.searchParams.set("allowWatchBookmarks", "true");
        // Add the abort signal to the request options
        opts.signal = this.#abortController.signal;
        console.log('kfc: buildURL:', url, opts);
        return { opts, url };
    };
    /** Run the watch. */
    #runner = async () => {
        console.log('kfc: runner');
        try {
            // Build the URL and request options
            const { opts, url } = await this.#buildURL();
            // Create a stream to read the response body
            this.#stream = byline_1.default.createStream();
            // Bind the stream events
            this.#stream.on("error", this.#errHandler);
            this.#stream.on("close", this.#cleanup);
            this.#stream.on("finish", this.#cleanup);
            // Make the actual request
            const response = await (0, node_fetch_1.default)(url, { ...opts });
            // Reset the pending reconnect flag
            this.#pendingReconnect = false;
            // Reset the resync timer
            this.#scheduleResync();
            // If the request is successful, start listening for events
            if (response.ok) {
                this.#events.emit(WatchEvent.CONNECT);
                const { body } = response;
                // Reset the retry count
                this.#retryCount = 0;
                // Listen for events and call the callback function
                this.#stream.on("data", async (line) => {
                    console.log('kfc: stream.on.data');
                    try {
                        // Parse the event payload
                        const { object: payload, type: phase } = JSON.parse(line);
                        this.#scheduleResync();
                        // If the watch is too old, remove the resourceVersion and reload the watch
                        if (phase === types_1.WatchPhase.Error && payload.code === 410) {
                            throw {
                                name: "TooOld",
                                message: this.#watchCfg.resourceVersion,
                            };
                        }
                        // If the event is a bookmark, emit the event and skip the callback
                        if (phase === types_1.WatchPhase.Bookmark) {
                            this.#events.emit(WatchEvent.BOOKMARK, payload);
                            console.log('kfc: got bookmark event: ', payload, phase);
                        }
                        else {
                            this.#events.emit(WatchEvent.DATA, payload, phase);
                            console.log('kfc: got data event: ', payload, phase);
                            // Call the callback function with the parsed payload
                            await this.#callback(payload, phase);
                        }
                        // Update the resource version if the callback was successful
                        this.#setResourceVersion(payload.metadata.resourceVersion);
                    }
                    catch (err) {
                        console.log('kfc: caught error: ', err);
                        if (err.name === "TooOld") {
                            // Prevent any body events from firing
                            body.removeAllListeners();
                            // Reload the watch
                            void this.#errHandler(err);
                            return;
                        }
                        console.log('kfc: data_error: ', err);
                        this.#events.emit(WatchEvent.DATA_ERROR, err);
                    }
                });
                // Bind the body events
                body.on("error", this.#errHandler);
                body.on("close", this.#cleanup);
                body.on("finish", this.#cleanup);
                // Pipe the response body to the stream
                body.pipe(this.#stream);
            }
            else {
                throw new Error(`watch connect failed: ${response.status} ${response.statusText}`);
            }
        }
        catch (e) {
            void this.#errHandler(e);
        }
    };
    /**
     * Resync the watch.
     *
     * @returns the error handler
     */
    #resync = () => {
        console.log('kfc: resync');
        this.#errHandler({
            name: "Resync",
            message: "Resync triggered by resyncIntervalSec",
        });
    };
    /** Clear the resync timer and schedule a new one. */
    #scheduleResync = () => {
        console.log('kfc: scheduleResync');
        console.log('kfc: clearing resyncTimer: ', this.#resyncTimer);
        clearTimeout(this.#resyncTimer);
        this.#resyncTimer = setTimeout(this.#resync, this.#watchCfg.resyncIntervalSec * 1000);
        console.log('kfc: new resyncTimer: ', this.#resyncTimer);
    };
    /**
     * Update the resource version.
     *
     * @param resourceVersion - the new resource version
     */
    #setResourceVersion = (resourceVersion) => {
        console.log('kfc: setResourceVersion: ', resourceVersion);
        this.#watchCfg.resourceVersion = resourceVersion;
        this.#events.emit(WatchEvent.RESOURCE_VERSION, resourceVersion);
    };
    /**
     * Reload the watch after an error.
     *
     * @param err - the error that occurred
     */
    #reconnect = async (err) => {
        console.log('kfc: reconnect: ', err);
        // If there are more attempts, retry the watch (undefined is unlimited retries)
        if (this.#watchCfg.retryMax === undefined || this.#watchCfg.retryMax > this.#retryCount) {
            // Sleep for the specified delay, but check every 500ms if the watch has been aborted
            let delay = this.#watchCfg.retryDelaySec * 1000;
            while (delay > 0) {
                if (this.#abortController.signal.aborted) {
                    return;
                }
                delay -= 500;
                await new Promise(resolve => setTimeout(resolve, 500));
            }
            this.#retryCount++;
            this.#events.emit(WatchEvent.RECONNECT, err, this.#retryCount);
            if (this.#pendingReconnect) {
                this.#events.emit(WatchEvent.RECONNECT_PENDING);
            }
            else {
                this.#pendingReconnect = true;
                this.#cleanup();
                // Retry the watch after the delay
                await this.#runner();
            }
        }
        else {
            // Otherwise, call the finally function if it exists
            this.#events.emit(WatchEvent.GIVE_UP, err);
            this.close();
        }
    };
    /**
     * Handle errors from the stream.
     *
     * @param err - the error that occurred
     */
    #errHandler = async (err) => {
        console.log('kfc: errHandler: ', err);
        switch (err.name) {
            case "AbortError":
                clearTimeout(this.#resyncTimer);
                this.#cleanup();
                this.#events.emit(WatchEvent.ABORT, err);
                return;
            case "TooOld":
                // Purge the resource version if it is too old
                this.#setResourceVersion(undefined);
                this.#events.emit(WatchEvent.OLD_RESOURCE_VERSION, err.message);
                break;
            case "Resync":
                this.#events.emit(WatchEvent.RESYNC, err);
                break;
            default:
                this.#events.emit(WatchEvent.NETWORK_ERROR, err);
                break;
        }
        await this.#reconnect(err);
    };
    /** Cleanup the stream and listeners. */
    #cleanup = () => {
        console.log('kfc: cleanup');
        if (this.#stream) {
            this.#stream.removeAllListeners();
            this.#stream.destroy();
        }
    };
}
exports.Watcher = Watcher;
