// src/config/sp.metadata.js
//
// sentences: 5 natural user-language sentences for multi-vector embedding.
//            Write these as DISTINCT as possible across SPs — overlapping
//            sentences cause wrong SP selection.
//
// defaults:  param values to inject when LLM returns null for critical params.
//            Prevents "A TOP or FETCH clause contains an invalid value" crashes.

export const SP_METADATA_HINTS = {
  // ── GetAllUsers ──────────────────────────────────────────────────────────────
  GetAllUsers: {
    sentences: [
      "Show me all users in the system.",
      "List every user account.",
      "Fetch all registered users.",
      "Get the complete list of system users.",
      "Who are all the users?",
    ],
    defaults: {},
  },

  // ── GetJobsAdvanced ──────────────────────────────────────────────────────────
  // Intentionally job-specific — no candidate/profile words
  GetJobsAdvanced: {
    sentences: [
      "Show me all available job openings.",
      "Search for jobs by title or keyword like react or manager.",
      "List jobs filtered by salary range, job type, or status.",
      "Get paginated job listings with total application count.",
      "Find job postings matching a specific search term.",
    ],
    defaults: {
      Page: 1,
      PageSize: 10,
    },
  },

  // ── usp_GetCandidateFullProfile ──────────────────────────────────────────────
  // Intentionally focused on ONE specific candidate by ID
  // No list/filter/all language — that belongs to ListWithFilters
  // CRITICAL: Every sentence MUST contain "ID" or "UUID".
  // NEVER use words: from, city, state, location, name, list, all, filter, verified.
  // This SP requires a UUID — it cannot search by name or location.
  usp_GetCandidateFullProfile: {
    sentences: [
      "Get full profile by candidate ID or UUID.",
      "Show detailed information for a candidate ID.",
      "Fetch resume and education using a specific candidate UUID.",
      "Retrieve work experience and skills by candidate ID.",
      "Show everything for a candidate identified by their unique ID.",
    ],
    defaults: {},
  },

  // ── usp_GetCandidateListWithFilters ──────────────────────────────────────────
  // Intentionally focused on LIST/FILTER — multiple candidates, not one
  usp_GetCandidateListWithFilters: {
    sentences: [
      "Show all candidates or search candidates by name.",
      "Find candidates from a city, state, or location like Ahmedabad or Gujarat.",
      "List verified candidates or candidates who completed their profile.",
      "Search candidates by name, email, city, state, or location.",
      "Get candidates from any location filtered by profile status or skill.",
    ],
    defaults: {
      PageNumber: 1,
      PageSize: 10,
    },
  },
};
