<script>
    import SearchBar from './SearchBar.svelte';
    import Results from './Results.svelte';
    import SimilarResults from './SimilarResults.svelte';

    const BASE_URL = 'http://172.206.100.189:8080';
    let query = '';
    let searchResults = [];
    let similarResults = [];
    let selectedTitleId = '';
    let isLoading = false;
    let hasError = false;

    async function performSearch() {
        isLoading = true;
        hasError = false;
        try {
            const response = await fetch(`${BASE_URL}/match`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ query })
            });

            if (response.ok) {
                searchResults = await response.json();
                similarResults = [];  // Reset similar results on new search
            } else {
                hasError = true;
            }
        } catch (error) {
            console.error('Error fetching data:', error);
            hasError = true;
        } finally {
            isLoading = false;
        }
    }

    async function fetchSimilar(titleId) {
        isLoading = true;
        hasError = false;
        try {
            selectedTitleId = titleId;
            const response = await fetch(`${BASE_URL}/similar`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ title_id: titleId, genres: ["Action"], original_query: query })
            });

            if (response.ok) {
                similarResults = await response.json();
            } else {
                hasError = true;
            }
        } catch (error) {
            console.error('Error fetching data:', error);
            hasError = true;
        } finally {
            isLoading = false;
        }
    }
</script>

<main>
    <SearchBar bind:query on:search={performSearch} />

    {#if isLoading}
        <p>Loading...</p>
    {:else if hasError}
        <p>Error loading data. Please try again.</p>
    {:else}
        <div class="content">
            <Results {searchResults} on:showSimilar={event => fetchSimilar(event.detail)} />
            {#if selectedTitleId}
                <SimilarResults {similarResults} />
            {/if}
        </div>
    {/if}
</main>

<style>
    main {
        padding: 20px;
        max-width: 800px;
        margin: auto;
    }

    .content {
        display: flex;
        gap: 20px;
    }
</style>
