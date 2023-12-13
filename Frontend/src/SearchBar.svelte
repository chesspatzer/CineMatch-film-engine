<script>
  import { createEventDispatcher } from 'svelte';
  export let query;

  const dispatch = createEventDispatcher();

  function handleSearch() {
      dispatch('search');
  }
</script>

<div class="search-bar">
  <input type="text" bind:value={query} placeholder="Search for a movie..." />
  <button on:click={handleSearch}>Search</button>
</div>

<style>
  .search-bar {
      display: flex;
      justify-content: center;
      margin-bottom: 20px;
  }

  input {
      padding: 8px;
      margin-right: 10px;
      flex-grow: 1;
      border-radius: 4px;
      border: 1px solid #ccc;
  }

  button {
      padding: 10px 15px;
      border-radius: 4px;
      border: none;
      background-color: #007bff;
      color: white;
      cursor: pointer;
  }

  button:hover {
      background-color: #0056b3;
  }
</style>
