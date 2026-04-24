export type BatchProgressEvent = {
  processed: number;
  total: number;
  currentFile?: string;
};

export type BatchResultRow = {
  queryFile: string;
  genomeId: number;
  score: number;
  matchedRibosomal: number;
  matchedTotal: number;
};

export type BatchResultsEvent = {
  rows: BatchResultRow[];
};

export type LogEvent = {
  level: "info" | "warn" | "error";
  message: string;
};

export type BuildProgressEvent =
  | { stage: "start"; allDb: string }
  | {
      stage: "genome";
      accession: string;
      genomesProcessed: number;
      peaksProcessed: number;
    }
  | { stage: "indexStart" }
  | { stage: "metaStart" }
  | { stage: "finalizeStart" }
  | {
      stage: "done";
      genomeCount: number;
      totalPeakCount: number;
      binCount: number;
    };

export type BuildStage = BuildProgressEvent["stage"];
