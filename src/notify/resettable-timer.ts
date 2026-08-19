
export class ResettableTimer<Args extends any[]> {
  private readonly callback: (...args: Args) => void;
  private delay: number;
  private timerId: NodeJS.Timeout | null = null;
  private args: Args;

  constructor(callback: (...args: Args) => void, delay: number, ...args: Args) {
    this.callback = callback;
    this.delay = delay;
    this.args = args;
  }

  /** Starts or restarts the timer */
  public start(): void {
    this.cancel();
    this.timerId = setTimeout(() => {
      this.callback(...this.args);
    }, this.delay);
  }

  /** Cancels the timer completely */
  public cancel(): void {
    if (this.timerId) {
      clearTimeout(this.timerId);
      this.timerId = null;
    }
  }

  /** Resets the timer back to the full delay duration */
  public reset(): void {
    this.start();
  }

  /** Optional: Update arguments or delay dynamically before running */
  public updateParams(delay: number, ...newArgs: Args): void {
    this.delay = delay;
    this.args = newArgs;
  }
}
