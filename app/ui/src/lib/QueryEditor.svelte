<script lang="ts">
  import { onMount, tick } from 'svelte';
  import * as ace from 'ace-builds';
  import 'ace-builds/src-noconflict/mode-sql';
  import 'ace-builds/src-noconflict/theme-chrome';
  import 'ace-builds/src-noconflict/theme-github_dark';

  interface Props {
    value: string;
    onrun: () => void;
    onprompt: () => void;
    promptOpen: boolean;
    promptText: string;
    promptError: string;
    promptErrorDetails: string;
    generatingPrompt: boolean;
    oncloseprompt: () => void;
    onsubmitprompt: () => void;
    onshowprompterror: () => void;
  }

  let {
    value = $bindable(''),
    onrun,
    onprompt,
    promptOpen = false,
    promptText = $bindable(''),
    promptError = '',
    promptErrorDetails = '',
    generatingPrompt = false,
    oncloseprompt,
    onsubmitprompt,
    onshowprompterror
  }: Props = $props();

  let container: HTMLDivElement;
  let promptInput = $state<HTMLTextAreaElement | null>(null);
  let editor: ace.Ace.Editor | null = null;
  let isApplyingExternalValue = false;

  function isDarkMode() {
    return window.matchMedia('(prefers-color-scheme: dark)').matches;
  }

  function applyTheme() {
    if (!editor) return;
    editor.setTheme(isDarkMode() ? 'ace/theme/github_dark' : 'ace/theme/chrome');
  }

  onMount(() => {
    const colorScheme = window.matchMedia('(prefers-color-scheme: dark)');

    editor = ace.edit(container, {
      mode: 'ace/mode/sql',
      theme: isDarkMode() ? 'ace/theme/github_dark' : 'ace/theme/chrome',
      value,
      fontSize: 14,
      showPrintMargin: false,
      highlightActiveLine: true,
      tabSize: 2,
      useSoftTabs: true,
      wrap: true,
    });

    editor.setOptions({
      enableBasicAutocompletion: false,
      enableLiveAutocompletion: false,
      showLineNumbers: true,
      showGutter: true,
      useWorker: false,
    });

    editor.commands.addCommand({
      name: 'runQuery',
      bindKey: { win: 'Ctrl-Enter', mac: 'Command-Enter' },
      exec: () => onrun(),
    });

    editor.commands.addCommand({
      name: 'promptQuery',
      bindKey: { win: 'Ctrl-I', mac: 'Command-I' },
      exec: () => onprompt(),
    });

    editor.session.on('change', () => {
      if (editor && !isApplyingExternalValue) {
        value = editor.getValue();
      }
    });

    const syncTheme = () => applyTheme();
    colorScheme.addEventListener('change', syncTheme);

    applyTheme();
    editor.focus();

    return () => {
      colorScheme.removeEventListener('change', syncTheme);
      editor?.destroy();
      editor = null;
    };
  });

  $effect(() => {
    if (!editor) return;
    const current = editor.getValue();
    if (current === value) return;
    isApplyingExternalValue = true;
    editor.setValue(value, -1);
    isApplyingExternalValue = false;
  });

  async function focusPromptInput() {
    await tick();
    promptInput?.focus();
  }

  $effect(() => {
    if (promptOpen) {
      void focusPromptInput();
    } else {
      editor?.focus();
    }
  });

  function handlePromptKeydown(event: KeyboardEvent) {
    if ((event.metaKey || event.ctrlKey) && event.key === 'Enter') {
      event.preventDefault();
      if (!generatingPrompt) {
        onsubmitprompt();
      }
      return;
    }

    if (event.key === 'Escape') {
      event.preventDefault();
      if (!generatingPrompt) {
        oncloseprompt();
      }
    }
  }
</script>

<div class="relative h-full w-full overflow-hidden bg-base-100">
  <div bind:this={container} class="h-full w-full"></div>

  {#if promptOpen}
    <div class="absolute inset-x-4 top-4 z-20">
      <div class="rounded-box border-base-300 bg-base-100/95 shadow-xl backdrop-blur">
        <div class="border-base-300 flex items-start justify-between gap-4 border-b px-4 py-3">
          <div>
            <h3 class="text-sm font-semibold">Generate Query</h3>
            <p class="text-base-content/70 mt-1 text-xs">
              원하는 쿼리를 설명하면 현재 편집기 내용을 참고해 SQL로 바꿉니다.
            </p>
          </div>
          <button
            class="btn btn-ghost btn-xs"
            type="button"
            onclick={oncloseprompt}
            disabled={generatingPrompt}
          >
            Close
          </button>
        </div>

        <div class="space-y-3 px-4 py-4">
          <textarea
            bind:this={promptInput}
            class="textarea h-32 w-full resize-none"
            placeholder="List the last 20 inserted rows from orders with customer email and total amount."
            bind:value={promptText}
            disabled={generatingPrompt}
            onkeydown={handlePromptKeydown}
          ></textarea>

          <div class="flex items-center justify-between gap-3 text-xs text-base-content/60">
            <span>생성된 SQL은 현재 에디터 내용을 대체합니다.</span>
            <div class="flex items-center gap-2">
              <kbd class="kbd kbd-xs">⌘</kbd>
              <span>+</span>
              <kbd class="kbd kbd-xs">Enter</kbd>
              <span>Generate</span>
              <span class="text-base-content/30">|</span>
              <kbd class="kbd kbd-xs">Esc</kbd>
              <span>Close</span>
            </div>
          </div>

          {#if promptError}
            <div role="alert" class="alert alert-error alert-soft flex items-center justify-between gap-3 text-sm">
              <span>{promptError}</span>
              {#if promptErrorDetails}
                <button class="btn btn-link btn-xs px-0" type="button" onclick={onshowprompterror}>
                  Details
                </button>
              {/if}
            </div>
          {/if}

          <div class="flex justify-end gap-2">
            <button
              class="btn btn-ghost btn-sm"
              type="button"
              onclick={oncloseprompt}
              disabled={generatingPrompt}
            >
              Cancel
            </button>
            <button
              class="btn btn-primary btn-sm"
              type="button"
              onclick={onsubmitprompt}
              disabled={generatingPrompt}
            >
              {#if generatingPrompt}
                <span class="loading loading-spinner loading-xs"></span>
              {:else}
                Generate
              {/if}
            </button>
          </div>
        </div>
      </div>
    </div>
  {/if}
</div>
