import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";

import type {
  BatchProgressEvent,
  BatchResultRow,
  BatchResultsEvent,
  BuildProgressEvent
} from "./lib/events";

const tauriEventMock = vi.hoisted(() => {
  let progressHandler:
    | ((event: { payload: BatchProgressEvent }) => void)
    | undefined;
  let resultsHandler:
    | ((event: { payload: BatchResultsEvent }) => void)
    | undefined;
  let buildProgressHandler:
    | ((event: { payload: BuildProgressEvent }) => void)
    | undefined;
  const unlisten = vi.fn();
  const listen = vi.fn(
    async (
      eventName: string,
      handler:
        | ((event: { payload: BatchProgressEvent }) => void)
        | ((event: { payload: BatchResultsEvent }) => void)
        | ((event: { payload: BuildProgressEvent }) => void)
    ) => {
      if (eventName === "batch-progress") {
        progressHandler = handler as (event: { payload: BatchProgressEvent }) => void;
      }
      if (eventName === "batch-results") {
        resultsHandler = handler as (event: { payload: BatchResultsEvent }) => void;
      }
      if (eventName === "build-progress") {
        buildProgressHandler = handler as (event: { payload: BuildProgressEvent }) => void;
      }
      return unlisten;
    }
  );

  return {
    listen,
    unlisten,
    getProgressHandler: () => progressHandler,
    getResultsHandler: () => resultsHandler,
    getBuildProgressHandler: () => buildProgressHandler
  };
});

vi.mock("@tauri-apps/api/event", () => ({
  listen: tauriEventMock.listen
}));

const dialogMock = vi.hoisted(() => ({
  directoryCallCount: 0,
  open: vi.fn(
    async ({
      directory,
      multiple
    }: {
      directory?: boolean;
      multiple?: boolean;
    }) => {
      if (multiple) {
        return ["/tmp/query-a.mgf", "/tmp/query-b.txt"];
      }

      if (directory) {
        dialogMock.directoryCallCount += 1;
        if (dialogMock.directoryCallCount === 1) {
          return "/tmp/small-db";
        }
        if (dialogMock.directoryCallCount === 2) {
          return "/tmp/R01-RS95";
        }
        return "/tmp/r01-rust-db";
      }

      return null;
    }
  ),
  save: vi.fn(async () => "/tmp/export.csv")
}));

vi.mock("@tauri-apps/plugin-dialog", () => dialogMock);

const apiMock = vi.hoisted(() => ({
  openDatabase: vi.fn(async () => {}),
  buildDatabaseFromSource: vi.fn(async () => "/tmp/r01-rust-db"),
  runIdentification: vi.fn(async () => {}),
  exportDatabaseToCsv: vi.fn(async () => {})
}));

vi.mock("./lib/api", () => apiMock);

import App from "./App";

it("renders streamed progress and reacts to backend progress events", async () => {
  render(<App />);

  expect(screen.getAllByText("已处理 0 / 0").length).toBeGreaterThan(0);

  await act(async () => {
    tauriEventMock.getProgressHandler()?.({
      payload: {
          processed: 1,
          total: 4,
          currentFile: "sample-a.mgf"
      }
    });
    tauriEventMock.getProgressHandler()?.({
      payload: {
        processed: 2,
        total: 4,
        currentFile: "sample-b.mgf"
      }
    });
  });

  expect(screen.getAllByText("已处理 2 / 4").length).toBeGreaterThan(0);
});

it("renders the redesigned workbench regions", () => {
  render(<App />);

  expect(screen.getByRole("heading", { name: "数据库" })).toBeInTheDocument();
  expect(screen.getByRole("heading", { name: "查询文件" })).toBeInTheDocument();
  expect(screen.getByRole("heading", { name: "运行日志" })).toBeInTheDocument();
  expect(
    screen.getByRole("heading", { name: "鉴定结果与处理状态" })
  ).toBeInTheDocument();
  expect(screen.getByText("处理状态")).toBeInTheDocument();
  expect(
    screen.getByText("集中显示结果数量、处理进度与当前状态，处理中的彩色轨迹会持续反馈。")
  ).toBeInTheDocument();
  expect(screen.getByRole("heading", { name: "结果表" })).toBeInTheDocument();
  expect(screen.getByTestId("query-panel-card")).toHaveClass("query-panel-card");
  expect(screen.getByTestId("result-table-card")).toHaveClass("result-card-fixed");
  expect(screen.getByTestId("result-table-card")).toHaveClass("workspace-card");
  expect(screen.getByTestId("result-table-card")).toHaveClass("result-card");
  expect(screen.queryByRole("button", { name: "导出 CSV" })).not.toBeInTheDocument();
  expect(screen.queryByText("核糖体匹配 / 总匹配")).not.toBeInTheDocument();
  expect(screen.getByText("匹配峰数")).toBeInTheDocument();
  expect(screen.queryByRole("heading", { name: "处理状态" })).not.toBeInTheDocument();
  expect(
    screen.queryByText(
      "将数据库加载、批量鉴定和结果查看整合进一个更接近 Claude 应用的浅色工作台。"
    )
  ).not.toBeInTheDocument();
});

it("keeps the merged status card responsive on narrow screens", () => {
  const styles = readFileSync(resolve(process.cwd(), "src/styles.css"), "utf8");

  expect(styles).toMatch(
    /@media\s*\(max-width:\s*980px\)\s*\{[\s\S]*?\.workbench-overview-card\s*\{\s*height:\s*auto;/
  );
});

it("surfaces build-progress stages via the persistent processing status panel", async () => {
  vi.useFakeTimers();
  render(<App />);

  expect(screen.getByTestId("processing-status-card")).toBeInTheDocument();
  expect(screen.getByText("处理状态")).toBeInTheDocument();

  await act(async () => {
    tauriEventMock.getBuildProgressHandler()?.({
      payload: { stage: "start", allDb: "/tmp/R01-RS95/mass/all.db" }
    });
  });

  expect(screen.getAllByText("处理中").length).toBeGreaterThan(0);
  expect(screen.getAllByText("正在准备构建").length).toBeGreaterThan(0);

  await act(async () => {
    tauriEventMock.getBuildProgressHandler()?.({
      payload: {
        stage: "genome",
        accession: "GCA_000001234.1",
        genomesProcessed: 1250,
        peaksProcessed: 8500
      }
    });
  });

  expect(screen.getAllByText("正在流式解码").length).toBeGreaterThan(0);
  expect(screen.getByText(/1,250 个基因组/)).toBeInTheDocument();
  expect(screen.getByText(/8,500 个峰值/)).toBeInTheDocument();
  expect(screen.getByText(/GCA_000001234\.1/)).toBeInTheDocument();

  await act(async () => {
    tauriEventMock.getBuildProgressHandler()?.({
      payload: { stage: "indexStart" }
    });
  });

  expect(screen.getAllByText("正在写入 mass_index.bin").length).toBeGreaterThan(0);

  await act(async () => {
    tauriEventMock.getBuildProgressHandler()?.({
      payload: { stage: "metaStart" }
    });
  });

  expect(screen.getAllByText("正在写入 meta.bin").length).toBeGreaterThan(0);

  await act(async () => {
    tauriEventMock.getBuildProgressHandler()?.({
      payload: { stage: "finalizeStart" }
    });
  });

  expect(screen.getAllByText("正在计算 CRC 并收尾").length).toBeGreaterThan(0);

  await act(async () => {
    tauriEventMock.getBuildProgressHandler()?.({
      payload: {
        stage: "done",
        genomeCount: 2048,
        totalPeakCount: 65536,
        binCount: 99
      }
    });
  });

  await act(async () => {
    vi.advanceTimersByTime(1_000);
  });

  expect(screen.getAllByText("已处理").length).toBeGreaterThan(0);
  vi.useRealTimers();
});

it("reacts to database selection and batch start button clicks", async () => {
  let now = 1_000;
  const nowSpy = vi.spyOn(Date, "now").mockImplementation(() => now);

  render(<App />);

  await act(async () => {
    fireEvent.click(screen.getByRole("button", { name: "选择数据库目录" }));
  });

  expect(dialogMock.open).toHaveBeenCalled();
  await waitFor(() => {
    expect(apiMock.openDatabase).toHaveBeenCalledWith("/tmp/small-db");
  });
  expect((await screen.findAllByText(/已加载数据库：/)).length).toBeGreaterThan(0);
  expect(
    screen.getByText("导入数据成功 · 已加载数据库 · 用时 0.00 秒")
  ).toBeInTheDocument();

  await act(async () => {
    fireEvent.click(screen.getByRole("button", { name: "导入原始数据库目录并构建" }));
  });

  await waitFor(() => {
    expect(apiMock.buildDatabaseFromSource).toHaveBeenCalledWith(
      "/tmp/R01-RS95",
      "/tmp/r01-rust-db"
    );
  });
  expect(await screen.findByText("已加载数据库：/tmp/r01-rust-db")).toBeInTheDocument();

  now = 4_000;
  await act(async () => {
    tauriEventMock.getBuildProgressHandler()?.({
      payload: { stage: "indexStart" }
    });
  });

  now = 6_300;
  await act(async () => {
    tauriEventMock.getBuildProgressHandler()?.({
      payload: { stage: "metaStart" }
    });
  });

  now = 8_050;
  await act(async () => {
    tauriEventMock.getBuildProgressHandler()?.({
      payload: {
        stage: "done",
        genomeCount: 2048,
        totalPeakCount: 65536,
        binCount: 99
      }
    });
  });

  expect(
    screen.getByText(
      "✅ 数据库构建完成！总耗时: 7.05s (流式解码: ~3.00s, 索引写入: ~2.30s)"
    )
  ).toBeInTheDocument();
  expect(
    screen.getByText("构建数据库成功 · 用时 7.05 秒")
  ).toBeInTheDocument();

  await act(async () => {
    fireEvent.click(screen.getByRole("button", { name: "选择查询文件" }));
  });

  await waitFor(() => {
    expect(screen.getByText("已选择 2 个查询文件")).toBeInTheDocument();
  });
  expect(
    screen.getByText("查询文件加载完成 · 共 2 个文件")
  ).toBeInTheDocument();

  now = 5_000;
  await act(async () => {
    fireEvent.click(screen.getByRole("button", { name: "开始批量鉴定" }));
  });

  await waitFor(() => {
    expect(apiMock.runIdentification).toHaveBeenCalledWith([
      "/tmp/query-a.mgf",
      "/tmp/query-b.txt"
    ]);
  });
  expect(await screen.findByText("批量鉴定已启动。")).toBeInTheDocument();

  await act(async () => {
    tauriEventMock.getProgressHandler()?.({
      payload: {
        processed: 3,
        total: 7,
        currentFile: "batch-a.mgf"
      }
    });
  });

  expect(screen.getAllByText("已处理 3 / 7").length).toBeGreaterThan(0);

  const rows: BatchResultRow[] = [
    {
      queryFile: "2026-04-21-long-query-file-name-that-needs-ellipsis-display.mgf",
      genomeId: 0,
      score: 0.91,
      matchedRibosomal: 1,
      matchedTotal: 2
    },
    {
      queryFile: "query-b.txt",
      genomeId: 1,
      score: 0.88,
      matchedRibosomal: 1,
      matchedTotal: 2
    }
  ];

  await act(async () => {
    now = 7_321;
    tauriEventMock.getResultsHandler()?.({
      payload: {
        rows
      }
    });
  });

  expect(screen.getByTestId("result-table-scroll")).toBeInTheDocument();
  expect(screen.getByRole("table")).toBeInTheDocument();
  expect(screen.getAllByText("查询完成").length).toBeGreaterThan(0);
  expect(screen.getByText("批量鉴定完成，用时 2.32 秒。")).toBeInTheDocument();
  expect(screen.getByText("批量鉴定完成 · 用时 2.32 秒")).toBeInTheDocument();

  await act(async () => {
    fireEvent.click(screen.getByRole("button", { name: "导出数据库 CSV" }));
  });

  await waitFor(() => {
    expect(apiMock.exportDatabaseToCsv).toHaveBeenCalledWith("/tmp/export.csv");
  });
  expect(await screen.findByText("数据库 CSV 已导出到 /tmp/export.csv")).toBeInTheDocument();
  expect(
    screen.getByText("导出数据库 CSV 成功 · 用时 0.00 秒")
  ).toBeInTheDocument();

  nowSpy.mockRestore();
});

it("keeps the processing glow active for at least one second before showing completion", async () => {
  vi.useFakeTimers();
  render(<App />);

  await act(async () => {
    tauriEventMock.getProgressHandler()?.({
      payload: {
        processed: 1,
        total: 1,
        currentFile: "single-query.mgf"
      }
    });
  });

  expect(screen.getByTestId("processing-status-card")).toHaveClass(
    "is-processing"
  );

  await act(async () => {
    vi.advanceTimersByTime(200);
    tauriEventMock.getResultsHandler()?.({
      payload: {
        rows: []
      }
    });
  });

  expect(screen.queryByText("查询完成")).not.toBeInTheDocument();

  await act(async () => {
    vi.advanceTimersByTime(799);
  });

  expect(screen.queryByText("查询完成")).not.toBeInTheDocument();

  await act(async () => {
    vi.advanceTimersByTime(1);
  });

  expect(screen.getAllByText("查询完成").length).toBeGreaterThan(0);
  expect(screen.getByTestId("processing-status-card")).not.toHaveClass(
    "is-processing"
  );

  vi.useRealTimers();
});
