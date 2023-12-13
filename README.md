# CineMatch Film Engine

CineMatch is a film recommendation engine. It uses an inverted index to map unique words to movie titles, allowing for fast and efficient search and recommendation capabilities.

## Project Structure

The project is divided into three main parts:

1. **Frontend**: This is the user interface of the application, built with Svelte. It includes components for search, results display, and similar results suggestions.

2. **Indexing**: This part of the application is responsible for creating the inverted index from a large dataset of movie titles. It is implemented in both Go and Python.

3. **Server**: This is the backend of the application, responsible for serving the frontend and handling API requests.
