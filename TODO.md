# To Do

- [ ] Common
  - [ ] Typing
    - This is a moving target...
  - [X] Time
  - [X] Templating
  - [ ] Logging

- [ ] Planning Engine
  - [X] Graph abstraction
    - [X] Vertex
    - [X] Cost
    - [X] Edge
    - [ ] Graph
      - Mostly completed, except for shortest path finding
  - [X] Templating implementation
  - [X] Transfer abstraction
    - [ ] Filesystem vertex
      - Mostly completed, except for querying interface and abstraction
        of identifying files by `stat` (a la `find`)
    - [X] Route transformation
    - [X] Route
    - [ ] Other abstractions
      - [X] Polynomial complexity cost
      - [ ] File (and combinations thereof) types
        - This is currently derived from `pathlib.Path`, it would be
          better to be URI-based
  - [ ] Implementation
    - [ ] Filesystems
      - [ ] POSIX
        - Completed, except for identification by `stat`, dependant upon
          abstraction
      - [ ] iRODS
        - Stubbed out for now...
      - [ ] S3
        - *Not for initial release...*
      - [ ] Google Cloud
        - *Not for initial release...*
    - [ ] Transformations
      - [X] Path prefixer
      - [X] Common path stripper
      - [X] Last n path component taker
      - [X] Telemetry wrapper
      - [X] Debugging wrapper
      - [ ] Archived structure canonicaliser
    - [ ] Routes
      - [X] POSIX to iRODS
      - [ ] iRODS to POSIX
      - [ ] POSIX to POSIX

- [ ] State Engine
  - [ ] ...

- [ ] Execution Engine
  - *Subtasks pending...*

- [ ] CLI
  - *Subtasks pending...*

- [ ] Documentation
  - [ ] Library documentation
  - [ ] CLI documentation

- [ ] Testing
  - *Chance would be a fine thing...*
