"""Eval pipeline: run all models on test cases and produce results."""

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

load_dotenv()

from eval.model_runner import MODEL_RUNNERS, run_model
from eval.scorer import ExecutionAccuracyScorer

CASES_DIR = Path(__file__).parent / "test_cases"
DEFAULT_CASES_PATH = CASES_DIR / "cases.json"
RESULTS_DIR = Path(__file__).parent / "results"


def load_test_cases(cases_path: Path | None = None) -> list[dict]:
    path = cases_path or DEFAULT_CASES_PATH
    with open(path) as f:
        return json.load(f)["test_cases"]


def run_eval(
    models: list[str] | None = None,
    verbose: bool = True,
    cases_path: Path | None = None,
    suite_name: str = "dev",
) -> dict:
    """Run the full evaluation pipeline.

    Args:
        models: subset of MODEL_RUNNERS keys to run (default: all).
        verbose: print per-case progress.
        cases_path: path to a test cases JSON. Defaults to cases.json (dev set).
            Pass eval/test_cases/holdout.json to run the holdout set.
        suite_name: label included in the output filename (e.g. "dev", "holdout").

    Returns a dict with per-model results and summary stats.
    """
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    scorer = ExecutionAccuracyScorer()
    cases = load_test_cases(cases_path)
    models = models or list(MODEL_RUNNERS.keys())

    all_results = {}

    for model_name in models:
        if verbose:
            print(f"\n{'='*60}")
            print(f"  Model: {model_name}")
            print(f"{'='*60}")

        model_results = []
        correct = 0
        total = len(cases)
        errors = []

        for i, case in enumerate(cases):
            case_id = case["id"]
            nl = case["natural_language"]
            gt_sql = case["ground_truth_sql"]

            if verbose:
                print(f"\n  [{i+1:2d}/{total}] {case_id}: {nl[:60]}...")

            result = run_model(model_name, nl)

            if "error" in result:
                score_result = None
                score_val = 0.0
                reason = f"API error: {result['error']}"
                if verbose:
                    print(f"         ERROR: {result['error'][:80]}")
            else:
                score_result = scorer.score(result["sql"], gt_sql)
                score_val = score_result.score
                reason = score_result.reason

                if score_val >= 1.0:
                    correct += 1
                    if verbose:
                        print(f"         PASS  ({score_result.reason})")
                else:
                    errors.append({
                        "case_id": case_id,
                        "category": case["category"],
                        "question": nl,
                        "predicted_sql": result["sql"][:200],
                        "score": score_val,
                        "reason": reason,
                    })
                    if verbose:
                        print(f"         FAIL  (score={score_val:.2f}, {reason})")
                        print(f"         SQL:  {result['sql'][:120]}...")

            model_results.append({
                "case_id": case_id,
                "category": case["category"],
                "difficulty": case["difficulty"],
                "natural_language": nl,
                "ground_truth_sql": gt_sql,
                "predicted_sql": result.get("sql", ""),
                "raw_output": result.get("raw", ""),
                "score": score_val,
                "reason": reason,
                "latency": result.get("latency", 0),
                "usage": result.get("usage", {}),
            })

            # Small delay to avoid rate limits
            time.sleep(0.3)

        accuracy = correct / total
        if verbose:
            print(f"\n  --- {model_name} Summary ---")
            print(f"  Accuracy: {correct}/{total} = {accuracy:.1%}")
            threshold = "PASS" if accuracy >= 0.85 else "FAIL"
            print(f"  Threshold (85%): {threshold}")

            if errors:
                print(f"\n  Failed cases:")
                for e in errors:
                    print(f"    {e['case_id']:15s} | {e['category']:25s} | {e['reason']}")

        # Per-category breakdown
        cat_stats = {}
        for r in model_results:
            cat = r["category"]
            cat_stats.setdefault(cat, {"correct": 0, "total": 0})
            cat_stats[cat]["total"] += 1
            if r["score"] >= 1.0:
                cat_stats[cat]["correct"] += 1

        all_results[model_name] = {
            "accuracy": accuracy,
            "correct": correct,
            "total": total,
            "threshold_pass": accuracy >= 0.85,
            "category_breakdown": {
                cat: {
                    "accuracy": s["correct"] / s["total"],
                    "correct": s["correct"],
                    "total": s["total"],
                }
                for cat, s in cat_stats.items()
            },
            "avg_latency": sum(r["latency"] for r in model_results) / len(model_results),
            "results": model_results,
            "errors": errors,
        }

    # Save results — suite_name lets dev and holdout runs land in different files.
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suite_tag = f"_{suite_name}" if suite_name and suite_name != "dev" else ""
    output_path = RESULTS_DIR / f"run{suite_tag}_{timestamp}.json"
    with open(output_path, "w") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False, default=str)

    if verbose:
        print(f"\n{'='*60}")
        print(f"  OVERALL SUMMARY")
        print(f"{'='*60}")
        for model_name, data in all_results.items():
            status = "PASS" if data["threshold_pass"] else "FAIL"
            print(f"  {model_name:20s}: {data['correct']}/{data['total']} = {data['accuracy']:.1%}  [{status}]")

        print(f"\n  Results saved to: {output_path}")

    return all_results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run the multi-model NL-to-SQL eval.")
    parser.add_argument(
        "--cases",
        type=str,
        default=None,
        help="Test cases JSON filename inside eval/test_cases/ (default: cases.json). "
             "Pass holdout.json to run the held-out set.",
    )
    parser.add_argument(
        "--models",
        type=str,
        nargs="*",
        default=None,
        help="Subset of model names to run (default: all).",
    )
    args = parser.parse_args()

    cases_path = (CASES_DIR / args.cases) if args.cases else None
    suite_name = Path(args.cases).stem if args.cases else "dev"
    run_eval(models=args.models, cases_path=cases_path, suite_name=suite_name)
