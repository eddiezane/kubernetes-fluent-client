// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2023-Present The Pepr Authors

import readline from "readline";

import fetch from "node-fetch";
import { GenericClass } from "../types";
import { Filters, WatchAction, WatchPhase } from "./types";
import { kubeCfg, pathBuilder } from "./utils";

/**
 * Execute a watch on the specified resource.
 */
export async function ExecWatch<T extends GenericClass>(
  model: T,
  filters: Filters,
  callback: WatchAction<T>,
) {
  // Build the path and query params for the resource, excluding the name
  const { opts, serverUrl } = await kubeCfg("GET");
  const url = pathBuilder(serverUrl, model, filters, true);

  // Enable the watch query param
  url.searchParams.set("watch", "true");

  // Allow bookmarks to be used for the watch
  url.searchParams.set("allowWatchBookmarks", "true");

  // If a name is specified, add it to the query params
  if (filters.name) {
    url.searchParams.set("fieldSelector", `metadata.name=${filters.name}`);
  }

  // Add abort controller to the long-running request
  const controller = new AbortController();
  opts.signal = controller.signal;

  // Close the connection and make the callback function no-op
  let close = (err?: Error) => {
    controller.abort();
    close = () => {};
    if (err) {
      throw err;
    }
  };

  try {
    // Make the actual request
    const response = await fetch(url, opts);

    // If the request is successful, start listening for events
    if (response.ok) {
      const { body } = response;

      // Bind connection events to the close function
      body.on("error", close);
      body.on("close", close);
      body.on("finish", close);

      // Create a readline interface to parse the stream
      const rl = readline.createInterface({
        input: response.body!,
        terminal: false,
      });

      // Listen for events and call the callback function
      rl.on("line", line => {
        try {
          // Parse the event payload
          const { object: payload, type: phase } = JSON.parse(line) as {
            type: WatchPhase;
            object: InstanceType<T>;
          };

          // Call the callback function with the parsed payload
          void callback(payload, phase as WatchPhase);
        } catch (ignore) {
          // ignore parse errors
        }
      });
    } else {
      // If the request fails, throw an error
      const error = new Error(response.statusText) as Error & {
        statusCode: number | undefined;
      };
      error.statusCode = response.status;
      throw error;
    }
  } catch (e) {
    close(e);
  }

  return controller;
}