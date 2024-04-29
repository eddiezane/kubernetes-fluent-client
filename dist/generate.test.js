"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const globals_1 = require("@jest/globals");
const generate_1 = require("./generate");
const fs_1 = __importDefault(require("fs"));
const sampleYaml = `
# non-crd should be ignored
apiVersion: v1
kind: ConfigMap
metadata:
  name: test
  namespace: default
data:
  any: bleh
---
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: movies.example.com
spec:
  group: example.com
  names:
    kind: Movie
    plural: movies
  scope: Namespaced
  versions:
    - name: v1
      schema:
        openAPIV3Schema:
          type: object
          description: Movie nerd
          properties:
            spec:
              properties:
                title:
                  type: string
                author:
                  type: string
              type: object
---
# duplicate entries should not break things
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: movies.example.com
spec:
  group: example.com
  names:
    kind: Movie
    plural: movies
  scope: Namespaced
  versions:
    - name: v1
      schema:
        openAPIV3Schema:
          type: object
          description: Movie nerd
          properties:
            spec:
              properties:
                title:
                  type: string
                author:
                  type: string
              type: object
---            
# should support multiple versions
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: books.example.com
spec:
  group: example.com
  names:
    kind: Book
    plural: books
  scope: Namespaced
  versions:
    - name: v1
      schema:
        openAPIV3Schema:
          type: object
          description: Book nerd
          properties:
            spec:
              properties:
                title:
                  type: string
                author:
                  type: string
              type: object
    - name: v2
      schema:
        openAPIV3Schema:
          type: object
          description: Book nerd
          properties:
            spec:
              properties:
                author:
                  type: string
              type: object
      served: true
      storage: true
`;
globals_1.jest.mock("./fetch", () => ({
    fetch: globals_1.jest.fn(),
}));
globals_1.jest.mock("./fluent", () => ({
    K8s: globals_1.jest.fn(),
}));
(0, globals_1.describe)("CRD Generate", () => {
    const originalReadFileSync = fs_1.default.readFileSync;
    globals_1.jest.spyOn(fs_1.default, "existsSync").mockReturnValue(true);
    globals_1.jest.spyOn(fs_1.default, "readFileSync").mockImplementation((...args) => {
        // Super janky hack due ot source-map-support calling readFileSync internally
        if (args[0].toString().includes("test-crd.yaml")) {
            return sampleYaml;
        }
        return originalReadFileSync(...args);
    });
    const mkdirSyncSpy = globals_1.jest.spyOn(fs_1.default, "mkdirSync").mockReturnValue(undefined);
    const writeFileSyncSpy = globals_1.jest.spyOn(fs_1.default, "writeFileSync").mockReturnValue(undefined);
    (0, globals_1.beforeEach)(() => {
        globals_1.jest.clearAllMocks();
    });
    (0, globals_1.test)("converts CRD to TypeScript", async () => {
        const options = { source: "test-crd.yaml", language: "ts", logFn: globals_1.jest.fn() };
        const actual = await (0, generate_1.generate)(options);
        const expectedMovie = [
            "// This file is auto-generated by kubernetes-fluent-client, do not edit manually\n",
            'import { GenericKind, RegisterKind } from "kubernetes-fluent-client";\n',
            "/**",
            " * Movie nerd",
            " */",
            "export class Movie extends GenericKind {",
            "    spec?: Spec;",
            "}",
            "",
            "export interface Spec {",
            "    author?: string;",
            "    title?:  string;",
            "}",
            "",
            "RegisterKind(Movie, {",
            '  group: "example.com",',
            '  version: "v1",',
            '  kind: "Movie",',
            "});",
        ];
        const expectedBookV1 = [
            "// This file is auto-generated by kubernetes-fluent-client, do not edit manually\n",
            'import { GenericKind, RegisterKind } from "kubernetes-fluent-client";\n',
            "/**",
            " * Book nerd",
            " */",
            "export class Book extends GenericKind {",
            "    spec?: Spec;",
            "}",
            "",
            "export interface Spec {",
            "    author?: string;",
            "    title?:  string;",
            "}",
            "",
            "RegisterKind(Book, {",
            '  group: "example.com",',
            '  version: "v1",',
            '  kind: "Book",',
            "});",
        ];
        const expectedBookV2 = expectedBookV1
            .filter(line => !line.includes("title?"))
            .map(line => line.replace("v1", "v2"));
        (0, globals_1.expect)(actual["movie-v1"]).toEqual(expectedMovie);
        (0, globals_1.expect)(actual["book-v1"]).toEqual(expectedBookV1);
        (0, globals_1.expect)(actual["book-v2"]).toEqual(expectedBookV2);
    });
    (0, globals_1.test)("converts CRD to TypeScript with plain option", async () => {
        const options = { source: "test-crd.yaml", language: "ts", plain: true, logFn: globals_1.jest.fn() };
        const actual = await (0, generate_1.generate)(options);
        const expectedMovie = [
            "/**",
            " * Movie nerd",
            " */",
            "export interface Movie {",
            "    spec?: Spec;",
            "}",
            "",
            "export interface Spec {",
            "    author?: string;",
            "    title?:  string;",
            "}",
            "",
        ];
        const expectedBookV1 = [
            "/**",
            " * Book nerd",
            " */",
            "export interface Book {",
            "    spec?: Spec;",
            "}",
            "",
            "export interface Spec {",
            "    author?: string;",
            "    title?:  string;",
            "}",
            "",
        ];
        const expectedBookV2 = expectedBookV1
            .filter(line => !line.includes("title?"))
            .map(line => line.replace("v1", "v2"));
        (0, globals_1.expect)(actual["movie-v1"]).toEqual(expectedMovie);
        (0, globals_1.expect)(actual["book-v1"]).toEqual(expectedBookV1);
        (0, globals_1.expect)(actual["book-v2"]).toEqual(expectedBookV2);
    });
    (0, globals_1.test)("converts CRD to TypeScript with other options", async () => {
        const options = {
            source: "test-crd.yaml",
            npmPackage: "test-package",
            logFn: globals_1.jest.fn(),
        };
        const actual = await (0, generate_1.generate)(options);
        const expectedMovie = [
            "// This file is auto-generated by test-package, do not edit manually\n",
            'import { GenericKind, RegisterKind } from "test-package";\n',
            "/**",
            " * Movie nerd",
            " */",
            "export class Movie extends GenericKind {",
            "    spec?: Spec;",
            "}",
            "",
            "export interface Spec {",
            "    author?: string;",
            "    title?:  string;",
            "}",
            "",
            "RegisterKind(Movie, {",
            '  group: "example.com",',
            '  version: "v1",',
            '  kind: "Movie",',
            "});",
        ];
        const expectedBookV1 = [
            "// This file is auto-generated by test-package, do not edit manually\n",
            'import { GenericKind, RegisterKind } from "test-package";\n',
            "/**",
            " * Book nerd",
            " */",
            "export class Book extends GenericKind {",
            "    spec?: Spec;",
            "}",
            "",
            "export interface Spec {",
            "    author?: string;",
            "    title?:  string;",
            "}",
            "",
            "RegisterKind(Book, {",
            '  group: "example.com",',
            '  version: "v1",',
            '  kind: "Book",',
            "});",
        ];
        const expectedBookV2 = expectedBookV1
            .filter(line => !line.includes("title?"))
            .map(line => line.replace("v1", "v2"));
        (0, globals_1.expect)(actual["movie-v1"]).toEqual(expectedMovie);
        (0, globals_1.expect)(actual["book-v1"]).toEqual(expectedBookV1);
        (0, globals_1.expect)(actual["book-v2"]).toEqual(expectedBookV2);
    });
    (0, globals_1.test)("converts CRD to TypeScript and writes to the given directory", async () => {
        const options = {
            source: "test-crd.yaml",
            directory: "test",
            logFn: globals_1.jest.fn(),
        };
        await (0, generate_1.generate)(options);
        const expectedMovie = [
            "// This file is auto-generated by kubernetes-fluent-client, do not edit manually\n",
            'import { GenericKind, RegisterKind } from "kubernetes-fluent-client";\n',
            "/**",
            " * Movie nerd",
            " */",
            "export class Movie extends GenericKind {",
            "    spec?: Spec;",
            "}",
            "",
            "export interface Spec {",
            "    author?: string;",
            "    title?:  string;",
            "}",
            "",
            "RegisterKind(Movie, {",
            '  group: "example.com",',
            '  version: "v1",',
            '  kind: "Movie",',
            "});",
        ];
        const expectedBookV1 = [
            "// This file is auto-generated by kubernetes-fluent-client, do not edit manually\n",
            'import { GenericKind, RegisterKind } from "kubernetes-fluent-client";\n',
            "/**",
            " * Book nerd",
            " */",
            "export class Book extends GenericKind {",
            "    spec?: Spec;",
            "}",
            "",
            "export interface Spec {",
            "    author?: string;",
            "    title?:  string;",
            "}",
            "",
            "RegisterKind(Book, {",
            '  group: "example.com",',
            '  version: "v1",',
            '  kind: "Book",',
            "});",
        ];
        const expectedBookV2 = expectedBookV1
            .filter(line => !line.includes("title?"))
            .map(line => line.replace("v1", "v2"));
        (0, globals_1.expect)(mkdirSyncSpy).toHaveBeenCalledWith("test", { recursive: true });
        (0, globals_1.expect)(writeFileSyncSpy).toHaveBeenCalledWith("test/movie-v1.ts", expectedMovie.join("\n"));
        (0, globals_1.expect)(writeFileSyncSpy).toHaveBeenCalledWith("test/book-v1.ts", expectedBookV1.join("\n"));
        (0, globals_1.expect)(writeFileSyncSpy).toHaveBeenCalledWith("test/book-v2.ts", expectedBookV2.join("\n"));
    });
    (0, globals_1.test)("converts CRD to Go", async () => {
        const options = { source: "test-crd.yaml", language: "go", logFn: globals_1.jest.fn() };
        const actual = await (0, generate_1.generate)(options);
        const expectedMovie = [
            "// Movie nerd",
            "type Movie struct {",
            '\tSpec *Spec `json:"spec,omitempty"`',
            "}",
            "",
            "type Spec struct {",
            '\tAuthor *string `json:"author,omitempty"`',
            '\tTitle  *string `json:"title,omitempty"`',
            "}",
            "",
        ];
        const expectedBookV1 = [
            "// Book nerd",
            "type Book struct {",
            '\tSpec *Spec `json:"spec,omitempty"`',
            "}",
            "",
            "type Spec struct {",
            '\tAuthor *string `json:"author,omitempty"`',
            '\tTitle  *string `json:"title,omitempty"`',
            "}",
            "",
        ];
        const expectedBookV2 = expectedBookV1
            .filter(line => !line.includes("Title"))
            .map(line => line.replace("v1", "v2"));
        (0, globals_1.expect)(actual["movie-v1"]).toEqual(expectedMovie);
        (0, globals_1.expect)(actual["book-v1"]).toEqual(expectedBookV1);
        (0, globals_1.expect)(actual["book-v2"]).toEqual(expectedBookV2);
    });
});
