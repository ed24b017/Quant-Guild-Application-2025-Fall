import argparse
import subprocess
import tempfile
from pathlib import Path

import pandas as pd
from tqdm import tqdm


def run_strategy(strategy_file: str, input_csv: Path, output_csv: Path) -> pd.DataFrame:
    subprocess.run(
        ["python3", strategy_file, "--input", str(input_csv), "--output", str(output_csv)],
        check=True,
        stdout=subprocess.DEVNULL,   # suppress normal prints
       # stderr=subprocess.DEVNULL    # suppress error messages too
    )
    return pd.read_csv(output_csv)
def test_forward_bias(strategy_file: str, full_data: pd.DataFrame, precision: int = 250, buffer: int = 5) -> bool:
    """
    Runs forward-bias test by comparing full-run vs partial runs.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Full run
        full_input = tmpdir / "full_input.csv"
        full_output = tmpdir / "full_output.csv"
        full_data.to_csv(full_input, index=False)
        full_signals = run_strategy(strategy_file, full_input, full_output)

        # Partial runs
        for i in tqdm(
        range(10, len(full_data), max(1, len(full_data)//precision)),
        desc="Forward Bias Check",
        unit="steps"):
            partial_input = tmpdir / f"partial_{i}.csv"
            partial_output = tmpdir / f"partial_{i}_out.csv"

            full_data.head(i).to_csv(partial_input, index=False)
            partial_signals = run_strategy(strategy_file, partial_input, partial_output)

            # Compare ignoring last buffer rows
            are_equal = full_signals.iloc[:i-buffer].equals(
                partial_signals.iloc[:i-buffer]
            )

            if not are_equal:
                tqdm.write(f"\n⚠️ Forward bias detected at index {i}")
                tqdm.write("Partial signals:\n" + str(partial_signals.tail(10)))
                tqdm.write("Full signals:\n" + str(full_signals.iloc[:i].tail(10)))

                return True
    return False


def main():
    parser = argparse.ArgumentParser(description="Test strategy for forward bias")
    parser.add_argument("--strategy", required=True, help="Path to strategy script")
    parser.add_argument("--prices", required=True, help="Path to prices CSV")
    parser.add_argument("--precision", type=int, default=100, help="Number of checkpoints")
    args = parser.parse_args()

    df = pd.read_csv(args.prices)

    if test_forward_bias(args.strategy, df, precision=args.precision):
        print("❌ Forward bias detected!")
    else:
        print("✅ No forward bias detected.")


if __name__ == "__main__":
    main()

