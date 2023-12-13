<script>
    import { createEventDispatcher } from 'svelte';
    export let searchResults = [];

    const dispatch = createEventDispatcher();

    function showSimilar(titleId) {
        dispatch('showSimilar', titleId);
    }
</script>

{#if searchResults.length > 0}


<div class="results">
    <h2>Query Results</h2>
    {#each searchResults as result}
        <div class="result-item">
            <p>{result.movie.primarytitle} ({result.movie.startyear})</p>
            <button on:click={() => showSimilar(result.movie.titleid)}>Show Similar</button>
        </div>
    {/each}
</div>
{/if}
<style>
    .results {
        margin-top: 20px;
    }

    .result-item {
        padding: 10px;
        border-bottom: 1px solid #eee;
    }

    button {
        padding: 5px 10px;
        border-radius: 4px;
        border: none;
        background-color: #28a745;
        color: white;
        cursor: pointer;
    }

    button:hover {
        background-color: #218838;
    }
</style>
