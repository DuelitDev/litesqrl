<script lang="ts">
  export let cols: string[];
  export let rows: string[][];
</script>

<div class="overflow-x-auto">
  <table class="table table-xs table-zebra">
    <thead>
      <tr>
        {#each cols as col (col)}
          <th>{col}</th>
        {/each}
      </tr>
    </thead>
    <tbody>
      {#each rows as row, rowIndex (rowIndex)}
        <tr>
          {#each row as cell, cellIndex (cellIndex)}
            <td>{cell}</td>
          {/each}
        </tr>
      {/each}
    </tbody>
  </table>
  <p class="text-base-content/50 mt-1 text-xs">{rows.length} row(s)</p>
</div>