from __future__ import annotations

from pathlib import Path

import yaml


def factory(name: str, **params):
    return {"object_type": name, **params}


def stage(
    stage_name: str,
    stage_kind: str,
    component_type: str,
    position: int,
    *,
    input_aliases: list[str],
    params: dict | None = None,
):
    return {
        "stage_name": stage_name,
        "stage_kind": stage_kind,
        "component_type": component_type,
        "params": params or {},
        "input_aliases": input_aliases,
        "position": position,
    }


def regex_hierarchy_pattern(level: int, pattern: str) -> dict[str, object]:
    return {"level": level, "pattern": pattern}


def retrieval(
    name: str,
    *,
    source_kind: str,
    retriever_type: str,
    queries: list[dict],
    params: dict | None = None,
    source_stage_name: str | None = None,
    requires_session: bool = False,
):
    source = {"kind": source_kind}
    if source_stage_name is not None:
        source["stage_name"] = source_stage_name
    return {
        "name": name,
        "source": source,
        "create": {
            "retriever_type": retriever_type,
            "params": params or {},
        },
        "requires_session": requires_session,
        "queries": queries,
    }


def query(name: str, query_text: str, top_k: int, *, strict_match: bool) -> dict:
    return {
        "name": name,
        "query": query_text,
        "top_k": top_k,
        "strict_match": strict_match,
    }


def _normalize_pipeline_create_payload(payload: dict) -> dict:
    normalized = dict(payload)
    indexing = normalized.get("indexing")
    if not isinstance(indexing, dict):
        return normalized

    indexing_copy = dict(indexing)
    params = indexing_copy.get("params")
    if isinstance(params, dict) and str(indexing_copy.get("index_type", "")).strip().lower() == "chroma":
        params_copy = dict(params)
        params_copy.pop("collection_name", None)
        params_copy.pop("doc_store_path", None)
        indexing_copy["params"] = params_copy
    normalized["indexing"] = indexing_copy
    return normalized


def run(
    run_name: str,
    pipeline_create_payload: dict,
    *,
    run_payload_template: dict | None = None,
    retrievals: list[dict] | None = None,
) -> dict:
    return {
        "run_name": run_name,
        "pipeline_create_payload": _normalize_pipeline_create_payload(pipeline_create_payload),
        "run_payload_template": run_payload_template or {},
        "retrievals": retrievals or [],
    }


def example(
    example_id: str,
    source_example_file: str,
    *,
    input_mode: str,
    input_spec: dict,
    project_create_payload: dict | None = None,
    runs: list[dict],
    expected_outcome: str = "success",
    notes: str = "",
) -> dict:
    return {
        "example_id": example_id,
        "source_example_file": source_example_file,
        "input_mode": input_mode,
        "input_spec": input_spec,
        "project_create_payload": project_create_payload or {},
        "runs": runs,
        "expected_outcome": expected_outcome,
        "notes": notes,
    }


LLM_NANO = factory(
    "create_llm",
    provider="openai",
    model_name="gpt-4.1-nano",
    temperature=0,
    streaming=False,
)
EMBEDDINGS_SMALL = factory(
    "create_embeddings_model",
    provider="openai",
    model_name="text-embedding-3-small",
)
TABLE_SUMMARIZER = factory("LLMTableSummarizer", llm=LLM_NANO)
NEO4J_GRAPH_STORE_CONFIG = {
    "provider": "neo4j",
    "params": {
        "uri": "bolt://neo4j:7687",
        "username": "neo4j",
        "password": "neo4j_password",
        "database": "neo4j",
    },
}
NEO4J_GRAPH_STORE = factory(
    "create_graph_store",
    provider="neo4j",
    uri="bolt://neo4j:7687",
    username="neo4j",
    password="neo4j_password",
    database="neo4j",
)

COMMON_NOTES = "Pipeline-only API example parity with rag-lib."

PLANTPAD_SEED_SCRIPT = """
async ({ keyword }) => {
  const current = new URL(window.location.href);
  if (!current.pathname.endsWith("/search.html")) {
    return false;
  }

  const app = document.querySelector("#app");
  const vm = app && app.__vue__;
  if (!vm) {
    return false;
  }

  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  if ((!Array.isArray(vm.search_result) || vm.search_result.length === 0) && typeof vm.doSearch === "function") {
    if (keyword && typeof vm.search_context === "string" && !vm.search_context.trim()) {
      vm.search_context = keyword;
      const searchInput = document.querySelector(".search_input");
      if (searchInput) {
        searchInput.value = keyword;
      }
    }
    vm.page = 0;
    vm.doSearch();
    await sleep(900);
  }

  if ((!Array.isArray(vm.search_result) || vm.search_result.length === 0) && typeof vm.getSearchData === "function") {
    vm.page = 0;
    vm.getSearchData();
    await sleep(900);
  }

  return true;
}
""".strip()

PLANTPAD_EXTRACT_SCRIPT = """
() => {
  const current = new URL(window.location.href);
  if (!current.pathname.endsWith("/search.html")) {
    return [];
  }
  const app = document.querySelector("#app");
  const vm = app && app.__vue__;
  if (!vm || !Array.isArray(vm.search_result)) {
    return [];
  }

  const urls = [];
  for (const item of vm.search_result) {
    const rawId = item && (item.img_id ?? item.imgId ?? item.id);
    if (rawId === undefined || rawId === null || rawId === "") {
      continue;
    }
    urls.push(`disease.html?img_id=${encodeURIComponent(String(rawId))}`);
  }
  return urls;
}
""".strip()

PLANTPAD_NEXT_PAGE_SCRIPT = """
() => {
  const current = new URL(window.location.href);
  if (!current.pathname.endsWith("/search.html")) {
    return false;
  }
  const app = document.querySelector("#app");
  const vm = app && app.__vue__;
  if (!vm || typeof vm.nextPage !== "function") {
    return false;
  }
  if (vm.top) {
    return false;
  }
  const before = Number(vm.page || 0);
  vm.nextPage();
  const after = Number(vm.page || 0);
  return after > before;
}
""".strip()

PLANTPAD_CLEANUP = factory(
    "WebCleanupConfig",
    duplicate_tags=["div", "p", "table"],
    non_recursive_classes=["tag"],
    navigation_classes=["menus"],
    navigation_styles=[],
    navigation_texts=["<", ">"],
    ignored_classes=["header"],
)
QUOTES_CLEANUP = factory(
    "WebCleanupConfig",
    duplicate_tags=[],
    non_recursive_classes=["tag"],
    navigation_classes=["side_categories", "pager"],
    ignored_classes=[
        "footer",
        "row header-box",
        "breadcrumb",
        "header container-fluid",
        "icon-star",
        "image_container",
    ],
)
EXAMPLE_COM_CLEANUP = factory(
    "WebCleanupConfig",
    duplicate_tags=["div", "p", "table"],
    non_recursive_classes=["tag"],
    navigation_classes=["side_categories", "pager"],
    ignored_classes=[
        "footer",
        "row header-box",
        "breadcrumb",
        "header container-fluid",
        "icon-star",
        "image_container",
    ],
)


EXAMPLES = [
    example(
        "01_text_basic",
        "01_text_basic.py",
        input_mode="file",
        input_spec={"file": "terms&defs.txt"},
        runs=[
            run(
                "main",
                {
                    "name": "01_text_basic_pipeline",
                    "loader": {"type": "TextLoader", "params": {}},
                    "inputs": [],
                    "stages": [
                        stage("logical_regex", "splitter", "RegexSplitter", 0, input_aliases=["LOADING"], params={"pattern": r"(?=##Term:)"}),
                        stage(
                            "token_chunks",
                            "splitter",
                            "TokenTextSplitter",
                            1,
                            input_aliases=["logical_regex"],
                            params={"chunk_size": 150, "chunk_overlap": 15, "model_name": "cl100k_base"},
                        ),
                    ],
                    "indexing": {
                        "index_type": "chroma",
                        "params": {
                            "embeddings": EMBEDDINGS_SMALL,
                            "cleanup": True,
                            "collection_name": "01_text_basic",
                            "dual_storage": True,
                            "doc_store_path": "./data/docstore/01_text_basic.pkl",
                        },
                        "collection_name": "01_text_basic",
                        "docstore_name": "01_text_basic_docstore",
                    },
                },
                retrievals=[
                    retrieval(
                        "dual",
                        source_kind="index",
                        retriever_type="create_scored_dual_storage_retriever",
                        params={
                            "id_key": "parent_id",
                            "search_kwargs": {"k": 10},
                            "search_type": "similarity_score_threshold",
                            "score_threshold": 0.5,
                        },
                        queries=[query("what_is_1c_crm", "Что такое 1C  CRM?", 10, strict_match=True)],
                    )
                ],
            )
        ],
        notes=COMMON_NOTES,
    ),
    example(
        "02_markdown_enrichment",
        "02_markdown_enrichment.py",
        input_mode="file",
        input_spec={"file": "quotes.toscrape.com_index.md"},
        runs=[
            run(
                "main",
                {
                    "name": "02_markdown_enrichment_pipeline",
                    "loader": {"type": "TextLoader", "params": {}},
                    "inputs": [],
                    "stages": [
                        stage(
                            "md_recursive",
                            "splitter",
                            "RecursiveCharacterTextSplitter",
                            0,
                            input_aliases=["LOADING"],
                            params={"chunk_size": 1000, "chunk_overlap": 100},
                        ),
                        stage(
                            "enriched",
                            "processor",
                            "SegmentEnricher",
                            1,
                            input_aliases=["md_recursive"],
                            params={"llm": LLM_NANO},
                        ),
                    ],
                },
                retrievals=[
                    retrieval(
                        "fuzzy",
                        source_kind="stage",
                        source_stage_name="enriched",
                        retriever_type="FuzzyRetriever",
                        params={"threshold": 45, "mode": "wratio"},
                        queries=[query("einstein", "einstein", 5, strict_match=True)],
                    )
                ],
            )
        ],
        notes=COMMON_NOTES,
    ),
    example(
        "02_markdown_enrichment_vector",
        "02_markdown_enrichment_vector.py",
        input_mode="file",
        input_spec={"file": "quotes.toscrape.com_index.md"},
        runs=[
            run(
                "main",
                {
                    "name": "02_markdown_enrichment_vector_pipeline",
                    "loader": {"type": "TextLoader", "params": {}},
                    "inputs": [],
                    "stages": [
                        stage(
                            "md_recursive",
                            "splitter",
                            "RecursiveCharacterTextSplitter",
                            0,
                            input_aliases=["LOADING"],
                            params={"chunk_size": 1000, "chunk_overlap": 100},
                        ),
                        stage(
                            "enriched",
                            "processor",
                            "SegmentEnricher",
                            1,
                            input_aliases=["md_recursive"],
                            params={"llm": LLM_NANO},
                        ),
                    ],
                    "indexing": {
                        "index_type": "faiss",
                        "params": {
                            "embeddings": EMBEDDINGS_SMALL,
                            "collection_name": "02_markdown_enrichment_vector",
                        },
                        "collection_name": "02_markdown_enrichment_vector",
                    },
                },
                retrievals=[
                    retrieval(
                        "vector",
                        source_kind="index",
                        retriever_type="create_vector_retriever",
                        params={"top_k": 2},
                        queries=[query("einstein_quotes", "einstein quotes", 2, strict_match=True)],
                    )
                ],
            )
        ],
        notes=COMMON_NOTES,
    ),
    example(
        "03_pdf_semantic",
        "03_pdf_semantic.py",
        input_mode="file",
        input_spec={"file": "2025_soo_frp_russkij-yazyk_10_11-2.pdf"},
        runs=[
            run(
                "main",
                {
                    "name": "03_pdf_semantic_pipeline",
                    "loader": {"type": "PyMuPDFLoader", "params": {"output_format": "markdown"}},
                    "inputs": [],
                    "stages": [
                        stage(
                            "structured",
                            "splitter",
                            "RegexHierarchySplitter",
                            0,
                            input_aliases=["LOADING"],
                            params={
                                "patterns": [
                                    regex_hierarchy_pattern(1, r"^\s*#\s+(.+)$"),
                                    regex_hierarchy_pattern(2, r"^\s*##\s+(.+)$"),
                                    regex_hierarchy_pattern(3, r"^\s*###\s+(.+)$"),
                                    regex_hierarchy_pattern(1, r"^\s*\*\*(.+?)\*\*\s*$"),
                                ],
                                "exclude_patterns": [r"^\s*\d+\s*$"],
                                "include_parent_content": False,
                            },
                        ),
                        stage(
                            "semantic",
                            "splitter",
                            "SemanticChunker",
                            1,
                            input_aliases=["structured"],
                            params={
                                "embeddings": EMBEDDINGS_SMALL,
                                "threshold_type": "fixed",
                                "threshold": 0.8,
                                "window_size": 4,
                            },
                        ),
                    ],
                    "indexing": {
                        "index_type": "chroma",
                        "params": {
                            "embeddings": EMBEDDINGS_SMALL,
                            "cleanup": True,
                            "collection_name": "03_pdf_semantic",
                        },
                        "collection_name": "03_pdf_semantic",
                    },
                },
                retrievals=[
                    retrieval(
                        "vector",
                        source_kind="index",
                        retriever_type="create_vector_retriever",
                        params={"top_k": 10},
                        queries=[query("morphology_vector", "Что такое морфология?", 10, strict_match=True)],
                    ),
                    retrieval(
                        "reranked",
                        source_kind="index",
                        retriever_type="create_reranking_retriever",
                        params={
                            "base_retriever_or_list": {"retriever_type": "create_vector_retriever", "params": {"top_k": 10}},
                            "top_k": 3,
                            "reranker_model": "BAAI/bge-reranker-v2-m3",
                            "max_score_ratio": 0.08,
                            "device": "cpu",
                        },
                        queries=[query("morphology_reranked", "Что такое морфология?", 3, strict_match=True)],
                    ),
                ],
            )
        ],
        notes=COMMON_NOTES,
    ),
    example(
        "04_pdf_raptor",
        "04_pdf_raptor.py",
        input_mode="file",
        input_spec={"file": "Georgy Volkov ru.pdf"},
        runs=[
            run(
                "main",
                {
                    "name": "04_pdf_raptor_pipeline",
                    "loader": {"type": "PDFLoader", "params": {"parse_mode": "text"}},
                    "inputs": [],
                    "stages": [
                        stage(
                            "sentences",
                            "splitter",
                            "SentenceSplitter",
                            0,
                            input_aliases=["LOADING"],
                            params={"chunk_size": 200, "chunk_overlap": 20, "language": "auto"},
                        ),
                        stage(
                            "raptor_tree",
                            "processor",
                            "RaptorProcessor",
                            1,
                            input_aliases=["sentences"],
                            params={"llm": LLM_NANO, "embeddings": EMBEDDINGS_SMALL, "max_levels": 3},
                        ),
                    ],
                    "indexing": {
                        "index_type": "chroma",
                        "params": {
                            "embeddings": EMBEDDINGS_SMALL,
                            "cleanup": True,
                            "collection_name": "04_pdf_raptor",
                            "dual_storage": True,
                            "doc_store_path": "./data/docstore/04_pdf_raptor.pkl",
                        },
                        "collection_name": "04_pdf_raptor",
                        "docstore_name": "04_pdf_raptor_docstore",
                    },
                },
                retrievals=[
                    retrieval(
                        "dual",
                        source_kind="index",
                        retriever_type="create_scored_dual_storage_retriever",
                        params={
                            "id_key": "parent_id",
                            "search_kwargs": {"k": 10},
                            "search_type": "similarity_score_threshold",
                            "hydration_mode": "children_enriched",
                        },
                        queries=[query("cio_europe", "CIO Europe", 10, strict_match=False)],
                    )
                ],
            )
        ],
        notes=COMMON_NOTES,
    ),
    example(
        "05_docx_graph",
        "05_docx_graph.py",
        input_mode="file",
        input_spec={"file": "Параметризованные задачи.docx"},
        project_create_payload={"graph_store_config": NEO4J_GRAPH_STORE_CONFIG},
        runs=[
            run(
                "main",
                {
                    "name": "05_docx_graph_pipeline",
                    "loader": {"type": "DocXLoader", "params": {}},
                    "inputs": [],
                    "stages": [
                        stage(
                            "docx_structure",
                            "splitter",
                            "RegexHierarchySplitter",
                            0,
                            input_aliases=["LOADING"],
                            params={
                                "patterns": [
                                    regex_hierarchy_pattern(1, r"^\s*#\s+(.+)$"),
                                    regex_hierarchy_pattern(2, r"^\s*##\s+(.+)$"),
                                    regex_hierarchy_pattern(3, r"^\s*###\s+(.+)$"),
                                    regex_hierarchy_pattern(4, r"^\s*####\s+(.+)$"),
                                ],
                                "exclude_patterns": [r"^\s*$"],
                                "include_parent_content": False,
                            },
                        ),
                        stage(
                            "graph_entities",
                            "processor",
                            "EntityExtractor",
                            1,
                            input_aliases=["docx_structure"],
                            params={"llm": LLM_NANO, "store": NEO4J_GRAPH_STORE},
                        ),
                    ],
                    "indexing": {
                        "index_type": "chroma",
                        "params": {
                            "embeddings": EMBEDDINGS_SMALL,
                            "cleanup": True,
                            "collection_name": "05_docx_graph",
                        },
                        "collection_name": "05_docx_graph",
                    },
                },
                retrievals=[
                    retrieval(
                        "graph_local",
                        source_kind="index",
                        retriever_type="GraphRetriever",
                        params={
                            "llm": LLM_NANO,
                            "config": factory(
                                "GraphQueryConfig",
                                mode="local",
                                max_hops=1,
                                top_k_entities=6,
                                top_k_relations=8,
                                top_k_chunks=6,
                                min_score=0.55,
                                token_budget_entities=450,
                                token_budget_relations=650,
                                token_budget_chunks=2400,
                                enable_keyword_extraction=True,
                                vector_relevance_mode="strict_0_1",
                            )
                        },
                        queries=[
                            query("probability_theory_local", "Теория вероятности", 10, strict_match=False),
                            query("probability_local", "вероятность", 10, strict_match=False),
                        ],
                    ),
                    retrieval(
                        "graph_mix",
                        source_kind="index",
                        retriever_type="GraphRetriever",
                        params={
                            "llm": LLM_NANO,
                            "config": factory(
                                "GraphQueryConfig",
                                mode="mix",
                                max_hops=1,
                                top_k_entities=6,
                                top_k_relations=10,
                                top_k_chunks=8,
                                min_score=0.50,
                                token_budget_entities=450,
                                token_budget_relations=700,
                                token_budget_chunks=2350,
                                enable_keyword_extraction=True,
                                vector_relevance_mode="strict_0_1",
                            )
                        },
                        queries=[
                            query("probability_theory_mix", "Теория вероятности", 10, strict_match=False),
                            query("probability_mix", "вероятность", 10, strict_match=False),
                        ],
                    ),
                    retrieval(
                        "graph_global",
                        source_kind="index",
                        retriever_type="GraphRetriever",
                        params={
                            "llm": LLM_NANO,
                            "config": factory(
                                "GraphQueryConfig",
                                mode="global",
                                max_hops=1,
                                top_k_entities=8,
                                top_k_relations=12,
                                top_k_chunks=6,
                                min_score=0.45,
                                token_budget_entities=600,
                                token_budget_relations=1200,
                                token_budget_chunks=1700,
                                enable_keyword_extraction=True,
                                vector_relevance_mode="strict_0_1",
                            )
                        },
                        queries=[
                            query("probability_theory_global", "Теория вероятности", 10, strict_match=False),
                            query("probability_global", "вероятность", 10, strict_match=False),
                        ],
                    ),
                    retrieval(
                        "graph_hybrid",
                        source_kind="index",
                        retriever_type="GraphRetriever",
                        params={
                            "llm": LLM_NANO,
                            "config": factory(
                                "GraphQueryConfig",
                                mode="hybrid",
                                max_hops=1,
                                top_k_entities=8,
                                top_k_relations=10,
                                top_k_chunks=7,
                                min_score=0.50,
                                token_budget_entities=550,
                                token_budget_relations=900,
                                token_budget_chunks=2000,
                                enable_keyword_extraction=True,
                                vector_relevance_mode="strict_0_1",
                            )
                        },
                        queries=[
                            query("probability_theory_hybrid", "Теория вероятности", 10, strict_match=False),
                            query("probability_hybrid", "вероятность", 10, strict_match=False),
                        ],
                    ),
                ],
            )
        ],
        notes=COMMON_NOTES,
    ),
    example(
        "06_docx_regex",
        "06_docx_regex.py",
        input_mode="file",
        input_spec={"file": "KP_IT_IB_Strategy_Recalc_v7_AppC.docx"},
        runs=[
            run(
                "main",
                {
                    "name": "06_docx_regex_pipeline",
                    "loader": {"type": "DocXLoader", "params": {}},
                    "inputs": [],
                    "stages": [
                        stage(
                            "docx_regex",
                            "splitter",
                            "RegexSplitter",
                            0,
                            input_aliases=["LOADING"],
                            params={
                                "pattern": r"(?m)(?=^(?:#\s+\d+\.\s+.+|##\s+\d+\.\d+\.\s+.+|###\s+Этап\s+(?:Э\d+|PA)\.\s+.+|\*\*(?:D\d+-\d+|PA-\d+)\.\s+.+\*\*|-\s+(?:D\d+-\d+|PA-\d+)\.\s+.+|\|\s*(?:D\d+-\d+|PA-\d+)\s*\|))",
                                "chunk_size": 1200,
                                "chunk_overlap": 0,
                            },
                        )
                    ],
                    "indexing": {
                        "index_type": "chroma",
                        "params": {
                            "embeddings": EMBEDDINGS_SMALL,
                            "cleanup": True,
                            "collection_name": "06_docx_regex",
                        },
                        "collection_name": "06_docx_regex",
                    },
                },
                retrievals=[
                    retrieval(
                        "regex",
                        source_kind="stage",
                        source_stage_name="docx_regex",
                        retriever_type="RegexRetriever",
                        queries=[
                            query("scope_of_work", "Состав работ", 5, strict_match=True),
                            query("team", "Команда", 5, strict_match=True),
                            query("effort", "трудозатраты", 5, strict_match=True),
                            query("phases", "этапы работ", 5, strict_match=True),
                            query("cost", "Стоимость", 5, strict_match=True),
                            query("goal", "Цель проекта", 5, strict_match=True),
                        ],
                    ),
                    retrieval(
                        "vector",
                        source_kind="index",
                        retriever_type="create_vector_retriever",
                        params={"top_k": 5},
                        queries=[
                            query("scope_of_work_vector", "Состав работ", 5, strict_match=True),
                            query("team_vector", "Команда", 5, strict_match=True),
                            query("effort_vector", "трудозатраты", 5, strict_match=True),
                            query("phases_vector", "этапы работ", 5, strict_match=True),
                            query("cost_vector", "Стоимость", 5, strict_match=True),
                            query("goal_vector", "Цель проекта", 5, strict_match=True),
                        ],
                    ),
                ],
            )
        ],
        notes=COMMON_NOTES,
    ),
    example(
        "07_csv_table_summary",
        "07_csv_table_summary.py",
        input_mode="file",
        input_spec={"file": "data.csv"},
        runs=[
            run(
                "main",
                {
                    "name": "07_csv_table_summary_pipeline",
                    "loader": {"type": "CSVLoader", "params": {"output_format": "csv"}},
                    "inputs": [],
                    "stages": [
                        stage(
                            "csv_tables",
                            "splitter",
                            "CSVTableSplitter",
                            0,
                            input_aliases=["LOADING"],
                            params={
                                "max_rows_per_chunk": 2,
                                "max_chunk_size": 500,
                                "summarizer": TABLE_SUMMARIZER,
                                "summarize_table": True,
                                "summarize_chunks": True,
                                "inject_summaries_into_content": True,
                            },
                        )
                    ],
                    "indexing": {
                        "index_type": "chroma",
                        "params": {
                            "embeddings": EMBEDDINGS_SMALL,
                            "cleanup": True,
                            "collection_name": "07_csv_table_summary",
                        },
                        "collection_name": "07_csv_table_summary",
                    },
                },
                retrievals=[
                    retrieval(
                        "vector",
                        source_kind="index",
                        retriever_type="create_vector_retriever",
                        params={"top_k": 3},
                        queries=[query("product_level_3", "Продукт уровень 3 ТУРИСТИЧЕСКАЯ", 3, strict_match=True)],
                    )
                ],
            )
        ],
        notes=COMMON_NOTES,
    ),
    example(
        "07_md_table_summary",
        "07_md_table_summary.py",
        input_mode="file",
        input_spec={"file": "07_md_table_summary_input_ru.md"},
        runs=[
            run(
                "main",
                {
                    "name": "07_md_table_summary_pipeline",
                    "loader": {"type": "TextLoader", "params": {}},
                    "inputs": [],
                    "stages": [
                        stage(
                            "md_tables",
                            "splitter",
                            "MarkdownTableSplitter",
                            0,
                            input_aliases=["LOADING"],
                            params={
                                "split_table_rows": True,
                                "max_rows_per_chunk": 1,
                                "summarizer": TABLE_SUMMARIZER,
                                "summarize_table": True,
                                "summarize_chunks": False,
                                "inject_summaries_into_content": True,
                            },
                        )
                    ],
                    "indexing": {
                        "index_type": "chroma",
                        "params": {
                            "embeddings": EMBEDDINGS_SMALL,
                            "cleanup": True,
                            "collection_name": "07_md_table_summary",
                        },
                        "collection_name": "07_md_table_summary",
                    },
                },
                retrievals=[
                    retrieval(
                        "vector",
                        source_kind="index",
                        retriever_type="create_vector_retriever",
                        params={"top_k": 3},
                        queries=[query("best_conversion_channel", "какой канал имеет лучшую конверсию", 3, strict_match=True)],
                    )
                ],
            )
        ],
        notes=COMMON_NOTES,
    ),
    example(
        "08_excel_csv_basic",
        "08_excel_csv_basic.py",
        input_mode="file",
        input_spec={"file": "08_result.xlsx"},
        runs=[
            run(
                "main",
                {
                    "name": "08_excel_csv_basic_pipeline",
                    "loader": {"type": "ExcelLoader", "params": {"output_format": "csv"}},
                    "inputs": [],
                    "stages": [
                        stage(
                            "excel_csv_chunks",
                            "splitter",
                            "CSVTableSplitter",
                            0,
                            input_aliases=["LOADING"],
                            params={"max_rows_per_chunk": 10, "max_chunk_size": 100},
                        )
                    ],
                    "indexing": {
                        "index_type": "chroma",
                        "params": {
                            "embeddings": EMBEDDINGS_SMALL,
                            "cleanup": True,
                            "collection_name": "08_excel_csv_basic",
                        },
                        "collection_name": "08_excel_csv_basic",
                    },
                },
                retrievals=[
                    retrieval(
                        "vector",
                        source_kind="index",
                        retriever_type="create_vector_retriever",
                        params={"top_k": 3},
                        queries=[query("vulnerability_testing_csv", "Тестирование уязвимостей", 3, strict_match=True)],
                    )
                ],
            )
        ],
        notes=COMMON_NOTES,
    ),
    example(
        "08_excel_md_basic",
        "08_excel_md_basic.py",
        input_mode="file",
        input_spec={"file": "08_result.xlsx"},
        runs=[
            run(
                "main",
                {
                    "name": "08_excel_md_basic_pipeline",
                    "loader": {"type": "ExcelLoader", "params": {}},
                    "inputs": [],
                    "stages": [
                        stage(
                            "excel_md_tables",
                            "splitter",
                            "MarkdownTableSplitter",
                            0,
                            input_aliases=["LOADING"],
                            params={"split_table_rows": True, "max_rows_per_chunk": 3},
                        )
                    ],
                    "indexing": {
                        "index_type": "chroma",
                        "params": {
                            "embeddings": EMBEDDINGS_SMALL,
                            "cleanup": True,
                            "collection_name": "08_excel_md_basic",
                        },
                        "collection_name": "08_excel_md_basic",
                    },
                },
                retrievals=[
                    retrieval(
                        "vector",
                        source_kind="index",
                        retriever_type="create_vector_retriever",
                        params={"top_k": 3},
                        queries=[query("vulnerability_testing_md", "Тестирование уязвимостей", 3, strict_match=True)],
                    )
                ],
            )
        ],
        notes=COMMON_NOTES,
    ),
    example(
        "09_json_hybrid",
        "09_json_hybrid.py",
        input_mode="file",
        input_spec={"file": "QA_data.json"},
        runs=[
            run(
                "main",
                {
                    "name": "09_json_hybrid_pipeline",
                    "loader": {
                        "type": "JsonLoader",
                        "params": {"output_format": "json", "schema": ".", "ensure_ascii": False},
                    },
                    "inputs": [],
                    "stages": [
                        stage(
                            "json_nodes",
                            "splitter",
                            "JsonSplitter",
                            0,
                            input_aliases=["LOADING"],
                            params={"schema": ".", "ensure_ascii": False},
                        )
                    ],
                    "indexing": {
                        "index_type": "chroma",
                        "params": {
                            "embeddings": EMBEDDINGS_SMALL,
                            "cleanup": True,
                            "collection_name": "09_json_hybrid",
                        },
                        "collection_name": "09_json_hybrid",
                    },
                },
                retrievals=[
                    retrieval(
                        "vector_basic",
                        source_kind="index",
                        retriever_type="create_vector_retriever",
                        params={"top_k": 3},
                        queries=[query("web_crm_basic", "WEB:CRM", 3, strict_match=True)],
                    ),
                    retrieval(
                        "vector_common_filter",
                        source_kind="index",
                        retriever_type="create_vector_retriever",
                        params={"top_k": 3, "filter": {"json_index": 0}},
                        queries=[query("web_crm_common_filter", "WEB:CRM", 3, strict_match=True)],
                    ),
                    retrieval(
                        "vector_json_filter",
                        source_kind="index",
                        retriever_type="create_vector_retriever",
                        params={"top_k": 3, "filter": {"json__metadata__it_system": "1С:CRM"}},
                        queries=[query("web_crm_json_filter", "WEB:CRM", 3, strict_match=True)],
                    ),
                ],
            )
        ],
        notes=COMMON_NOTES,
    ),
    example(
        "10_text_ensemble",
        "10_text_ensemble.py",
        input_mode="file",
        input_spec={"file": "terms&defs.txt"},
        runs=[
            run(
                "main",
                {
                    "name": "10_text_ensemble_pipeline",
                    "loader": {"type": "TextLoader", "params": {}},
                    "inputs": [],
                    "stages": [
                        stage(
                            "sentences",
                            "splitter",
                            "SentenceSplitter",
                            0,
                            input_aliases=["LOADING"],
                            params={"chunk_size": 300, "chunk_overlap": 30, "language": "auto"},
                        )
                    ],
                    "indexing": {
                        "index_type": "chroma",
                        "params": {
                            "embeddings": EMBEDDINGS_SMALL,
                            "cleanup": True,
                            "collection_name": "10_text_ensemble",
                        },
                        "collection_name": "10_text_ensemble",
                    },
                },
                retrievals=[
                    retrieval(
                        "bm25",
                        source_kind="stage",
                        source_stage_name="sentences",
                        retriever_type="create_bm25_retriever",
                        params={"top_k": 3},
                        queries=[query("cost_of_risk_bm25", "Cost of risk", 3, strict_match=True)],
                    ),
                    retrieval(
                        "vector",
                        source_kind="index",
                        retriever_type="create_vector_retriever",
                        params={"top_k": 3},
                        queries=[query("cost_of_risk_vector", "Cost of risk", 3, strict_match=True)],
                    ),
                    retrieval(
                        "ensemble",
                        source_kind="index",
                        retriever_type="create_ensemble_retriever",
                        params={
                            "retrievers": [
                                {"retriever_type": "create_bm25_retriever", "params": {"top_k": 3}},
                                {"retriever_type": "create_vector_retriever", "params": {"top_k": 3}},
                            ],
                            "weights": [0.5, 0.5],
                        },
                        queries=[query("cost_of_risk_ensemble", "Cost of risk", 3, strict_match=True)],
                    ),
                ],
            )
        ],
        notes=COMMON_NOTES,
    ),
    example(
        "11_log_regex_loader",
        "11_log_regex_loader.py",
        input_mode="file",
        input_spec={"file": "anon.log"},
        runs=[
            run(
                "main",
                {
                    "name": "11_log_regex_loader_pipeline",
                    "loader": {
                        "type": "RegexHierarchyLoader",
                        "params": {
                            "patterns": [regex_hierarchy_pattern(1, r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")]
                        },
                    },
                    "inputs": [],
                    "stages": [
                        stage(
                            "log_hierarchy",
                            "splitter",
                            "RegexHierarchySplitter",
                            0,
                            input_aliases=["LOADING"],
                            params={
                                "patterns": [regex_hierarchy_pattern(1, r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")]
                            },
                        )
                    ],
                    "indexing": {
                        "index_type": "chroma",
                        "params": {
                            "embeddings": EMBEDDINGS_SMALL,
                            "cleanup": True,
                            "collection_name": "11_log_regex_loader",
                        },
                        "collection_name": "11_log_regex_loader",
                    },
                },
                retrievals=[
                    retrieval(
                        "vector",
                        source_kind="index",
                        retriever_type="create_vector_retriever",
                        params={"top_k": 3},
                        queries=[query("credit_card", "Credit card", 3, strict_match=True)],
                    )
                ],
            )
        ],
        notes=COMMON_NOTES,
    ),
    example(
        "12_qa_loader",
        "12_qa_loader.py",
        input_mode="file",
        input_spec={"file": "interview.txt"},
        runs=[
            run(
                "main",
                {
                    "name": "12_qa_loader_pipeline",
                    "loader": {"type": "TextLoader", "params": {}},
                    "inputs": [],
                    "stages": [
                        stage("qa_pairs", "splitter", "QASplitter", 0, input_aliases=["LOADING"], params={})
                    ],
                    "indexing": {
                        "index_type": "chroma",
                        "params": {
                            "embeddings": EMBEDDINGS_SMALL,
                            "cleanup": True,
                            "collection_name": "12_qa_loader",
                        },
                        "collection_name": "12_qa_loader",
                    },
                },
                retrievals=[
                    retrieval(
                        "vector",
                        source_kind="index",
                        retriever_type="create_vector_retriever",
                        params={"top_k": 3},
                        queries=[query("graph_database_experience", "graph database experience", 3, strict_match=True)],
                    )
                ],
            )
        ],
        notes=COMMON_NOTES,
    ),
    example(
        "13_dual_storage",
        "13_dual_storage.py",
        input_mode="segments",
        input_spec={
            "segments": [
                {
                    "segment_id": "doc_rag",
                    "content": "Retrieval Augmented Generation (RAG) combines retrieval and generation. A retriever finds relevant external context and the generator uses that context to produce grounded responses with lower hallucination risk.",
                    "metadata": {"title": "RAG Definition", "topic": "rag"},
                    "type": "text",
                    "original_format": "text",
                    "path": ["dual_storage_demo.txt"],
                    "level": 0,
                },
                {
                    "segment_id": "doc_indexing",
                    "content": "Dual storage keeps compact searchable chunks in a vector index while storing full parent documents in a document store. Retrieval finds chunk matches first and then hydrates the associated parent document.",
                    "metadata": {"title": "Dual Storage Indexing", "topic": "indexing"},
                    "type": "text",
                    "original_format": "text",
                    "path": ["dual_storage_demo.txt"],
                    "level": 0,
                },
            ]
        },
        runs=[
            run(
                "main",
                {
                    "name": "13_dual_storage_pipeline",
                    "runtime_input": {"alias": "PARENTS", "artifact_kind": "segment"},
                    "inputs": [],
                    "stages": [
                        stage(
                            "token_chunks",
                            "splitter",
                            "TokenTextSplitter",
                            0,
                            input_aliases=["PARENTS"],
                            params={"chunk_size": 35, "chunk_overlap": 8, "model_name": "cl100k_base"},
                        )
                    ],
                    "indexing": {
                        "index_type": "chroma",
                        "params": {
                            "embeddings": EMBEDDINGS_SMALL,
                            "cleanup": True,
                            "collection_name": "13_dual_storage",
                            "dual_storage": True,
                            "doc_store_path": "./data/docstore/13_dual_storage.pkl",
                        },
                        "collection_name": "13_dual_storage",
                        "docstore_name": "13_dual_storage_docstore",
                    },
                },
                retrievals=[
                    retrieval(
                        "dual",
                        source_kind="index",
                        retriever_type="create_scored_dual_storage_retriever",
                        params={
                            "id_key": "parent_id",
                            "search_kwargs": {"k": 5},
                            "search_type": "similarity_score_threshold",
                            "score_threshold": 0.0,
                        },
                        queries=[
                            query(
                                "what_is_rag",
                                "What is retrieval augmented generation and why use it?",
                                5,
                                strict_match=True,
                            )
                        ],
                    )
                ],
            )
        ],
        notes=COMMON_NOTES,
    ),
    example(
        "14_mineru_pdf",
        "14_mineru_pdf.py",
        input_mode="file",
        input_spec={"file": "statement.pdf"},
        runs=[
            run(
                "main",
                {
                    "name": "14_mineru_pdf_pipeline",
                    "loader": {
                        "type": "MinerULoader",
                        "params": {
                            "parse_mode": "txt",
                            "timeout_seconds": 1200,
                            "start_page": 0,
                            "end_page": 4,
                            "parse_formula": False,
                            "parse_table": False,
                        },
                    },
                    "inputs": [],
                    "stages": [
                        stage(
                            "mineru_recursive",
                            "splitter",
                            "RecursiveCharacterTextSplitter",
                            0,
                            input_aliases=["LOADING"],
                            params={"chunk_size": 1200, "chunk_overlap": 120},
                        )
                    ],
                    "indexing": {
                        "index_type": "chroma",
                        "params": {
                            "embeddings": EMBEDDINGS_SMALL,
                            "cleanup": True,
                            "collection_name": "14_mineru_pdf",
                        },
                        "collection_name": "14_mineru_pdf",
                    },
                },
                retrievals=[
                    retrieval(
                        "vector",
                        source_kind="index",
                        retriever_type="create_vector_retriever",
                        params={"top_k": 3},
                        queries=[query("statement_date", "statement date", 3, strict_match=False)],
                    )
                ],
            )
        ],
        notes=COMMON_NOTES,
    ),
    example(
        "15_pptx_unsupported",
        "15_pptx_unsupported.py",
        input_mode="file",
        input_spec={"file": "Digitme Презентация.pptx"},
        runs=[
            run(
                "main",
                {
                    "name": "15_pptx_unsupported_pipeline",
                    "loader": {"type": "PPTXLoader", "params": {}},
                    "inputs": [],
                    "stages": [],
                }
            )
        ],
        expected_outcome="error",
        notes="Expected pipeline creation failure because rag-lib exposes no PPTXLoader.",
    ),
    example(
        "16_html_html",
        "16_html_html.py",
        input_mode="file",
        input_spec={"file": "15_test.html"},
        runs=[
            run(
                "main",
                {
                    "name": "16_html_html_pipeline",
                    "loader": {"type": "HTMLLoader", "params": {"output_format": "html"}},
                    "inputs": [],
                    "stages": [
                        stage(
                            "html_blocks",
                            "splitter",
                            "HTMLSplitter",
                            0,
                            input_aliases=["LOADING"],
                            params={
                                "output_format": "html",
                                "split_table_rows": True,
                                "max_rows_per_chunk": 6,
                                "use_first_row_as_header": True,
                                "include_parent_content": False,
                            },
                        ),
                        stage(
                            "html_child_chunks",
                            "splitter",
                            "RecursiveCharacterTextSplitter",
                            1,
                            input_aliases=["html_blocks"],
                            params={"chunk_size": 1000, "chunk_overlap": 120},
                        ),
                    ],
                    "indexing": {
                        "index_type": "chroma",
                        "params": {
                            "embeddings": EMBEDDINGS_SMALL,
                            "cleanup": True,
                            "collection_name": "16_html_html",
                            "dual_storage": True,
                            "doc_store_path": "./data/docstore/16_html_html.pkl",
                        },
                        "collection_name": "16_html_html",
                        "docstore_name": "16_html_html_docstore",
                    },
                },
                retrievals=[
                    retrieval(
                        "dual",
                        source_kind="index",
                        retriever_type="create_scored_dual_storage_retriever",
                        params={
                            "id_key": "parent_id",
                            "search_kwargs": {"k": 6},
                            "search_type": "similarity_score_threshold",
                            "score_threshold": 0.0,
                        },
                        queries=[query("damaged_packaging", "Which order has damaged packaging?", 6, strict_match=True)],
                    )
                ],
            )
        ],
        notes=COMMON_NOTES,
    ),
    example(
        "16_html_md",
        "16_html_md.py",
        input_mode="file",
        input_spec={"file": "15_test.html"},
        runs=[
            run(
                "main",
                {
                    "name": "16_html_md_pipeline",
                    "loader": {"type": "HTMLLoader", "params": {"output_format": "html"}},
                    "inputs": [],
                    "stages": [
                        stage(
                            "html_blocks",
                            "splitter",
                            "HTMLSplitter",
                            0,
                            input_aliases=["LOADING"],
                            params={
                                "output_format": "markdown",
                                "split_table_rows": True,
                                "max_rows_per_chunk": 6,
                                "use_first_row_as_header": True,
                                "include_parent_content": False,
                            },
                        ),
                        stage(
                            "html_child_chunks",
                            "splitter",
                            "RecursiveCharacterTextSplitter",
                            1,
                            input_aliases=["html_blocks"],
                            params={"chunk_size": 1000, "chunk_overlap": 120},
                        ),
                    ],
                    "indexing": {
                        "index_type": "chroma",
                        "params": {
                            "embeddings": EMBEDDINGS_SMALL,
                            "cleanup": True,
                            "collection_name": "16_html_md",
                            "dual_storage": True,
                            "doc_store_path": "./data/docstore/16_html_md.pkl",
                        },
                        "collection_name": "16_html_md",
                        "docstore_name": "16_html_md_docstore",
                    },
                },
                retrievals=[
                    retrieval(
                        "dual",
                        source_kind="index",
                        retriever_type="create_scored_dual_storage_retriever",
                        params={
                            "id_key": "parent_id",
                            "search_kwargs": {"k": 6},
                            "search_type": "similarity_score_threshold",
                            "score_threshold": 0.0,
                        },
                        queries=[query("project_stage", "Which project belongs to A. Novak and what stage is it in?", 6, strict_match=True)],
                    )
                ],
            )
        ],
        notes=COMMON_NOTES,
    ),
    example(
        "17A_web_loader_plantpad",
        "17A_web_loader_plantpad.py",
        input_mode="url",
        input_spec={"url": "https://plantpad.samlab.cn/search.html"},
        runs=[
            run(
                "sync",
                {
                    "name": "17A_web_loader_plantpad_sync",
                    "loader": {
                        "type": "WebLoader",
                        "params": {
                            "depth": 3,
                            "output_format": "markdown",
                            "fetch_mode": "playwright",
                            "crawl_scope": "same_host",
                            "follow_download_links": False,
                            "ignore_https_errors": True,
                            "cleanup_config": PLANTPAD_CLEANUP,
                            "playwright_headless": True,
                            "playwright_navigation_config": factory(
                                "PlaywrightNavigationConfig",
                                enabled=True,
                                max_clicks=512,
                                max_states=513,
                            ),
                            "playwright_extraction_config": factory(
                                "PlaywrightExtractionConfig",
                                profiles=[
                                    factory(
                                        "PlaywrightProfileConfig",
                                        profile="paginated_eval",
                                        script_args={"keyword": ""},
                                        seed_script=PLANTPAD_SEED_SCRIPT,
                                        extract_script=PLANTPAD_EXTRACT_SCRIPT,
                                        next_page_script=PLANTPAD_NEXT_PAGE_SCRIPT,
                                        max_pages=512,
                                        wait_after_action_ms=700,
                                        source_tag="vue-search",
                                        source_classes=["table-button"],
                                    )
                                ],
                            ),
                        },
                    },
                    "inputs": [],
                    "stages": [],
                }
            ),
            run(
                "async",
                {
                    "name": "17A_web_loader_plantpad_async",
                    "loader": {
                        "type": "AsyncWebLoader",
                        "params": {
                            "depth": 3,
                            "output_format": "markdown",
                            "fetch_mode": "playwright",
                            "crawl_scope": "same_host",
                            "follow_download_links": False,
                            "max_concurrency": 4,
                            "ignore_https_errors": True,
                            "cleanup_config": PLANTPAD_CLEANUP,
                            "playwright_headless": True,
                            "playwright_navigation_config": factory(
                                "PlaywrightNavigationConfig",
                                enabled=True,
                                max_clicks=512,
                                max_states=513,
                            ),
                            "playwright_extraction_config": factory(
                                "PlaywrightExtractionConfig",
                                profiles=[
                                    factory(
                                        "PlaywrightProfileConfig",
                                        profile="paginated_eval",
                                        script_args={"keyword": ""},
                                        seed_script=PLANTPAD_SEED_SCRIPT,
                                        extract_script=PLANTPAD_EXTRACT_SCRIPT,
                                        next_page_script=PLANTPAD_NEXT_PAGE_SCRIPT,
                                        max_pages=512,
                                        wait_after_action_ms=700,
                                        source_tag="vue-search",
                                        source_classes=["table-button"],
                                    )
                                ],
                            ),
                        },
                    },
                    "inputs": [],
                    "stages": [],
                }
            ),
        ],
        notes=COMMON_NOTES,
    ),
    example(
        "17B_web_loader_quotes",
        "17B_web_loader_quotes.py",
        input_mode="url",
        input_spec={"url": "https://quotes.toscrape.com"},
        runs=[
            run(
                "sync",
                {
                    "name": "17B_web_loader_quotes_sync",
                    "loader": {
                        "type": "WebLoader",
                        "params": {
                            "depth": 3,
                            "output_format": "markdown",
                            "fetch_mode": "requests",
                            "crawl_scope": "same_host",
                            "follow_download_links": False,
                            "cleanup_config": QUOTES_CLEANUP,
                        },
                    },
                    "inputs": [],
                    "stages": [],
                }
            ),
            run(
                "async",
                {
                    "name": "17B_web_loader_quotes_async",
                    "loader": {
                        "type": "AsyncWebLoader",
                        "params": {
                            "depth": 3,
                            "output_format": "markdown",
                            "fetch_mode": "requests_fallback_playwright",
                            "crawl_scope": "same_host",
                            "follow_download_links": False,
                            "max_concurrency": 4,
                            "cleanup_config": QUOTES_CLEANUP,
                        },
                    },
                    "inputs": [],
                    "stages": [],
                }
            ),
        ],
        notes=COMMON_NOTES,
    ),
    example(
        "17C_web_loader_example",
        "17C_web_loader_example.py",
        input_mode="url",
        input_spec={"url": "https://example.com"},
        runs=[
            run(
                "sync",
                {
                    "name": "17C_web_loader_example_sync",
                    "loader": {
                        "type": "WebLoader",
                        "params": {
                            "depth": 2,
                            "output_format": "markdown",
                            "fetch_mode": "requests",
                            "crawl_scope": "allow_all",
                            "follow_download_links": False,
                            "cleanup_config": EXAMPLE_COM_CLEANUP,
                            "ignore_https_errors": True,
                        },
                    },
                    "inputs": [],
                    "stages": [],
                }
            ),
            run(
                "async",
                {
                    "name": "17C_web_loader_example_async",
                    "loader": {
                        "type": "AsyncWebLoader",
                        "params": {
                            "depth": 2,
                            "output_format": "markdown",
                            "fetch_mode": "requests",
                            "crawl_scope": "allow_all",
                            "follow_download_links": False,
                            "max_concurrency": 4,
                            "cleanup_config": EXAMPLE_COM_CLEANUP,
                            "ignore_https_errors": True,
                        },
                    },
                    "inputs": [],
                    "stages": [],
                }
            ),
        ],
        notes=COMMON_NOTES,
    ),
    example(
        "17_web_loader",
        "17_web_loader.py",
        input_mode="url",
        input_spec={"url": "https://quotes.toscrape.com"},
        runs=[
            run(
                "sync",
                {
                    "name": "17_web_loader_sync",
                    "loader": {
                        "type": "WebLoader",
                        "params": {
                            "depth": 2,
                            "output_format": "markdown",
                            "fetch_mode": "requests",
                            "crawl_scope": "same_host",
                            "follow_download_links": False,
                        },
                    },
                    "inputs": [],
                    "stages": [],
                }
            ),
            run(
                "async",
                {
                    "name": "17_web_loader_async",
                    "loader": {
                        "type": "AsyncWebLoader",
                        "params": {
                            "depth": 2,
                            "output_format": "markdown",
                            "fetch_mode": "requests_fallback_playwright",
                            "crawl_scope": "same_host",
                            "follow_download_links": False,
                            "max_concurrency": 4,
                        },
                    },
                    "inputs": [],
                    "stages": [],
                }
            ),
        ],
        notes=COMMON_NOTES,
    ),
]


def main() -> int:
    manifest = {"version": "v2", "examples": EXAMPLES}
    output_path = Path("examples/pipeline_examples/manifest.v1.yaml")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        yaml.safe_dump(manifest, sort_keys=False, allow_unicode=True, width=120),
        encoding="utf-8",
    )
    print(output_path.as_posix())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

