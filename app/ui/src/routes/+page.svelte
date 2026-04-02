<script lang="ts">
  import Icon from '@iconify/svelte';
  import { invoke } from '@tauri-apps/api/core';
  import { onMount } from 'svelte';
  import {
    generateQueryFromPrompt,
    loadAiSettings,
    saveAiSettings,
    type AiSettings
  } from '$lib/ai';
  import QueryEditor from '$lib/QueryEditor.svelte';
  import QueryCountResult from '$lib/result/QueryCountResult.svelte';
  import QueryErrorResult from '$lib/result/QueryErrorResult.svelte';
  import QueryRowsResult from '$lib/result/QueryRowsResult.svelte';
  import QuerySuccessResult from '$lib/result/QuerySuccessResult.svelte';

  type QueryResult =
    | { type: 'Success' }
    | { type: 'Count'; data: number }
    | { type: 'Rows'; data: { columns: string[]; rows: string[][] } }
    | { type: 'Err'; data: string };

  let query = $state('');
  let results = $state<QueryResult[]>([]);
  let running = $state(false);
  let elapsedMs = $state<number | null>(null);
  let settingsDialog = $state<HTMLDialogElement | null>(null);
  let aiSettings = $state<AiSettings>({ apiKey: '', endpoint: '', model: '', lang: '' });
  let settingsLoading = $state(true);
  let settingsSaving = $state(false);
  let settingsError = $state('');
  let promptOpen = $state(false);
  let promptText = $state('');
  let promptError = $state('');
  let promptErrorDetails = $state('');
  let generatingQuery = $state(false);
  let detailsDialog = $state<HTMLDialogElement | null>(null);
  let detailsTitle = $state('');
  let detailsContent = $state('');

  onMount(async () => {
    try {
      const saved = await loadAiSettings();
      if (saved) {
        aiSettings.apiKey = saved.apiKey;
        aiSettings.endpoint = saved.endpoint;
        aiSettings.model = saved.model;
        aiSettings.lang = saved.lang;
      }

      if (!aiSettings.lang.trim()) {
        aiSettings.lang = navigator.language || 'en';
      }
    } catch (error) {
      settingsError = error instanceof Error ? error.message : 'Failed to load AI settings.';
    } finally {
      settingsLoading = false;
    }
  });

  async function run() {
    if (!query.trim()) return;
    const startedAt = performance.now();
    running = true;
    results = await invoke<QueryResult[]>('run_query', { src: query });
    elapsedMs = Math.round(performance.now() - startedAt);
    running = false;
  }

  function openSettings() {
    settingsError = '';
    settingsDialog?.showModal();
  }

  function openPromptDialog() {
    promptError = '';
    promptErrorDetails = '';
    promptOpen = true;
  }

  function closePromptInline() {
    if (generatingQuery) return;
    promptOpen = false;
  }

  function formatErrorDetails(error: unknown): string {
    if (error instanceof Error) {
      const parts = [error.name, error.message, error.stack].filter(Boolean);
      return parts.join('\n\n');
    }

    if (typeof error === 'string') {
      return error;
    }

    if (error && typeof error === 'object') {
      try {
        return JSON.stringify(error, null, 2);
      } catch {
        return String(error);
      }
    }

    return String(error);
  }

  function openDetails(title: string, content: string) {
    detailsTitle = title;
    detailsContent = content;
    detailsDialog?.showModal();
  }

  async function generateQuery() {
    promptError = '';
    promptErrorDetails = '';

    if (!promptText.trim()) {
      promptError = 'Prompt is required.';
      return;
    }

    generatingQuery = true;

    try {
      const generatedQuery = await generateQueryFromPrompt(promptText, query);
      if (!generatedQuery.trim()) {
        throw new Error('The AI response did not contain SQL.');
      }
      query = generatedQuery;
      promptOpen = false;
    } catch (error) {
      promptError = 'Failed to generate a query.';
      promptErrorDetails = formatErrorDetails(error);
    } finally {
      generatingQuery = false;
    }
  }

  async function saveSettings() {
    settingsError = '';

    if (!aiSettings.apiKey.trim()) {
      settingsError = 'API key is required.';
      return;
    }

    if (!aiSettings.endpoint.trim()) {
      settingsError = 'Endpoint is required.';
      return;
    }

    if (!aiSettings.model.trim()) {
      settingsError = 'Model is required.';
      return;
    }

    if (!aiSettings.lang.trim()) {
      settingsError = 'LANG is required.';
      return;
    }

    settingsSaving = true;

    try {
      await saveAiSettings({
        apiKey: aiSettings.apiKey.trim(),
        endpoint: aiSettings.endpoint.trim(),
        model: aiSettings.model.trim(),
        lang: aiSettings.lang.trim()
      });
      settingsDialog?.close();
    } catch (error) {
      settingsError = error instanceof Error ? error.message : 'Failed to save AI settings.';
    } finally {
      settingsSaving = false;
    }
  }
</script>

<div class="flex h-screen flex-col">
  <div class="flex min-h-0 flex-1">
    <!-- Left Pane: Query Editor -->
    <div class="border-base-300 flex w-1/2 flex-col border-r">
      <div class="border-base-300 flex items-center border-b px-4 py-2">
        <h2 class="text-md font-semibold">Query</h2>
      </div>
      <div class="relative flex-1">
        <QueryEditor
          bind:value={query}
          bind:promptText={promptText}
          promptOpen={promptOpen}
          promptError={promptError}
          promptErrorDetails={promptErrorDetails}
          generatingPrompt={generatingQuery}
          onrun={run}
          onprompt={openPromptDialog}
          oncloseprompt={closePromptInline}
          onsubmitprompt={generateQuery}
          onshowprompterror={() => openDetails('Query Generation Error', promptErrorDetails)}
        />
        <div class="pointer-events-none absolute right-4 bottom-4 z-10">
          <button
            class="btn btn-success btn-circle btn-md pointer-events-auto shadow-lg"
            disabled={running || !query.trim()}
            onclick={run}
          >
            {#if running}
              <span class="loading loading-spinner loading-xs"></span>
            {:else}
              <Icon icon="lucide:play" width={20} height={20} />
            {/if}
          </button>
        </div>
      </div>
    </div>
    <!-- Right: Result -->
    <div class="flex w-1/2 flex-col">
      <div class="border-base-300 border-b px-4 py-2">
        <h2 class="text-md font-semibold">Result</h2>
      </div>
      <div class="flex-1 overflow-auto p-4 font-mono text-sm">
        {#if results.length === 0}
          <span class="text-base-content/40">Run a query to see results.</span>
        {:else}
          {#each results as result, i (i)}
            <div class="mb-2">
              {#if result.type === 'Success'}
                <QuerySuccessResult />
              {:else if result.type === 'Count'}
                <QueryCountResult count={result.data} />
              {:else if result.type === 'Rows'}
                <QueryRowsResult cols={result.data.columns} rows={result.data.rows} />
              {:else if result.type === 'Err'}
                <QueryErrorResult
                  message={result.data}
                  query={query}
                  lang={aiSettings.lang}
                />
              {/if}
            </div>
          {/each}
        {/if}
      </div>
    </div>
  </div>

  <div
    class="border-base-300 bg-base-200 text-base-content flex items-center justify-between border-t px-4 py-2 text-sm"
  >
    <div class="flex items-center gap-2 font-medium">
      <kbd class="kbd kbd-sm">⌘</kbd>
      <span>+</span>
      <kbd class="kbd kbd-sm">Enter</kbd>
      <span class="text-base-content/70">to run query</span>
      <span class="text-base-content/30">|</span>
      <kbd class="kbd kbd-sm">⌘</kbd>
      <span>+</span>
      <kbd class="kbd kbd-sm">I</kbd>
      <span class="text-base-content/70">to prompt AI</span>
    </div>
    <div class="flex items-center gap-2">
      <div class="text-base-content/70 min-w-16 text-right font-mono">
        {#if running}
          Running...
        {:else if elapsedMs !== null}
          {elapsedMs} ms
        {:else}
          -- ms
        {/if}
      </div>
      <button
        class="btn btn-ghost btn-xs btn-square"
        aria-label="Open AI settings"
        onclick={openSettings}
      >
        <Icon icon="lucide:settings-2" width={14} height={14} />
      </button>
    </div>
  </div>

  <dialog bind:this={settingsDialog} class="modal">
    <div class="modal-box max-w-md">
      <h3 class="text-base font-semibold">Generative AI</h3>
      <p class="text-base-content/70 mt-2 text-sm">
        Configure the API key and chat completions endpoint for the GUI assistant.
      </p>

      <div class="mt-4 space-y-4">
        <fieldset class="fieldset">
          <legend class="fieldset-legend">Endpoint</legend>
          <input
            class="input w-full font-mono text-xs"
            type="url"
            placeholder="https://api.openai.com/v1/chat/completions"
            bind:value={aiSettings.endpoint}
            autocomplete="off"
            disabled={settingsLoading || settingsSaving}
          />
          <p class="label">Saved to the app settings file as entered.</p>
        </fieldset>

        <fieldset class="fieldset">
          <legend class="fieldset-legend">API Key</legend>
          <input
            class="input w-full"
            type="password"
            placeholder="sk-..."
            bind:value={aiSettings.apiKey}
            autocomplete="off"
            disabled={settingsLoading || settingsSaving}
          />
          <p class="label">Stored locally for this app.</p>
        </fieldset>

        <fieldset class="fieldset">
          <legend class="fieldset-legend">Model</legend>
          <input
            class="input w-full font-mono text-xs"
            type="text"
            placeholder="gpt-4o, claude-sonnet-4-20250514, etc."
            bind:value={aiSettings.model}
            autocomplete="off"
            disabled={settingsLoading || settingsSaving}
          />
        </fieldset>

        <fieldset class="fieldset">
          <legend class="fieldset-legend">Language</legend>
          <input
            class="input w-full"
            placeholder="ko, en, ja, de, etc."
            bind:value={aiSettings.lang}
            autocomplete="off"
            disabled={settingsLoading || settingsSaving}
          />
          <p class="label">The AI uses this language for explanations.</p>
        </fieldset>

        {#if settingsError}
          <div role="alert" class="alert alert-error alert-soft text-sm">{settingsError}</div>
        {/if}
      </div>

      <div class="modal-action">
        <form method="dialog">
          <button class="btn btn-ghost">Cancel</button>
        </form>
        <button class="btn btn-primary" onclick={saveSettings} disabled={settingsLoading || settingsSaving}>
          {#if settingsSaving}
            <span class="loading loading-spinner loading-xs"></span>
          {:else}
            Save
          {/if}
        </button>
      </div>
    </div>
    <form method="dialog" class="modal-backdrop">
      <button>close</button>
    </form>
  </dialog>

  <dialog bind:this={detailsDialog} class="modal">
    <div class="modal-box max-w-2xl">
      <h3 class="text-base font-semibold">{detailsTitle}</h3>
      <div class="bg-base-200 rounded-box mt-4 whitespace-pre-wrap px-3 py-3 font-mono text-sm leading-6">
        {detailsContent}
      </div>

      <div class="modal-action">
        <form method="dialog">
          <button class="btn btn-ghost">Close</button>
        </form>
      </div>
    </div>
    <form method="dialog" class="modal-backdrop">
      <button>close</button>
    </form>
  </dialog>
</div>
