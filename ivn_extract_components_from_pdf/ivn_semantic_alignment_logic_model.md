```mermaid
graph TD
    subgraph Inputs
        A[Executive Order Components Excel File <br> (ivn_components_...xlsx)]
        B[IVN Database Excel File <br> (USDA-IVN-dataset.xlsx)]
    end

    subgraph Activities
        C{Load Data} --> D{Perform Semantic Search};
        D --> E{Vectorize Text (TF-IDF)};
        E --> F{Calculate Cosine Similarity};
        F --> G{Identify Best Matches};
    end

    subgraph Outputs
        H[Semantic Alignments CSV File <br> (ivn_semantic_alignments_...csv)]
    end

    subgraph Outcomes
        I[Structured, evidence-based mapping between EO components and IVN components]
        J[Identification of gaps and overlaps]
    end

    A --> C;
    B --> C;
    G --> H;
    H --> I;
    H --> J;
```
