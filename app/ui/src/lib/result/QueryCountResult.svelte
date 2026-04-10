<script lang="ts">
  export let count: number;
</script>

<div class="alert alert-success alert-soft px-2 py-1">
  {count} row{count === 1 ? '' : 's'} affected
</div>