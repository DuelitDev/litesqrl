<script lang="ts">
  import Icon from '@iconify/svelte';
  import { explainQueryError } from '$lib/ai';

  interface Props {
    message: string;
    query: string;
    lang: string;
  }

  let { message, query, lang }: Props = $props();

  let expanded = $state(false);
  let explaining = $state(false);
  let explanationError = $state('');
  let errorExplanation = $state('');

  async function handleExplain() {
    if (expanded && !explaining) {
      expanded = false;
      return;
    }

    expanded = true;
    errorExplanation = '';
    explanationError = '';
    explaining = true;

    try {
      errorExplanation = await explainQueryError(query, message, lang);
    } catch (error) {
      explanationError = error instanceof Error ? error.message : 'Failed to explain the error.';
    } finally {
      explaining = false;
    }
  }
</script>

<div class="alert alert-error alert-soft px-2 py-1">
  <div class="w-full space-y-3">
    <div class="flex gap-3">
      <button class="btn btn-xs btn-ghost" onclick={handleExplain}>
        {#if explaining}
          <span class="loading loading-spinner loading-xs"></span>
        {:else}
          <Icon icon="lucide:stars" width={16} height={16} />
        {/if}
      </button>
      <span class="items-center">{message}</span>
    </div>

    {#if expanded}
      <div class="border-error/20 bg-base-100/60 rounded-box border px-3 py-3 text-sm leading-6">
        {#if explaining}
          <div class="flex items-center gap-2 text-sm">
            <span class="loading loading-spinner loading-sm"></span>
            <span>Diagnosing the error...</span>
          </div>
        {:else if explanationError}
          <div role="alert" class="alert alert-error alert-soft text-sm">{explanationError}</div>
        {:else if errorExplanation}
          <div class="bg-base-100/70 rounded-box whitespace-pre-wrap px-3 py-3 text-sm leading-6">
            {errorExplanation}
          </div>
        {/if}
      </div>
    {/if}
  </div>
</div>