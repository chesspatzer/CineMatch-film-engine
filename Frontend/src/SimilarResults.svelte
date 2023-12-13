<script>
    export let similarResults = [];
</script>

{#if similarResults.length > 0}
    <div class="similar-results">
        <h2>Similar Movies</h2>
        {#each similarResults as result}
            <div class="similar-item">
                <p>{result.movie.primarytitle} ({result.movie.startyear})</p>
            </div>
        {/each}
    </div>
{:else}
    <p>No similar movies found.</p>
{/if}

<style>
    .similar-results {
        margin-top: 20px;
    }

    .similar-item {
        padding: 10px;
        border-bottom: 1px solid #eee;
    }
</style>
