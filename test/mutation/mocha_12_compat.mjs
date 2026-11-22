import Module from 'node:module';

const CLI_MODULES = new Map([
    ['/lib/cli/run-helpers', '/lib/cli/run-helpers.cjs'],
    ['/lib/cli/collect-files', '/lib/cli/collect-files.cjs'],
    ['/lib/cli/options', '/lib/cli/options.cjs']
]);

const originalResolveFilename = Module._resolveFilename;

// Stryker 10.1 resolves these private Mocha modules by their pre-v12 names.
Module._resolveFilename = function resolveMochaCliModule(request, parent, isMain, options) {
    for (const [suffix, replacement] of CLI_MODULES) {
        if (request.endsWith(suffix)) request = request.slice(0, -suffix.length) + replacement;
    }
    return originalResolveFilename.call(this, request, parent, isMain, options);
};

let mochaRunner;
try {
    mochaRunner = await import('@stryker-mutator/mocha-runner');
} finally {
    Module._resolveFilename = originalResolveFilename;
}

export const strykerPlugins = mochaRunner.strykerPlugins;
export const strykerValidationSchema = mochaRunner.strykerValidationSchema;
