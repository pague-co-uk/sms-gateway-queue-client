export class QueueProcessingError
  extends Error {
  public readonly requeue: boolean;

  constructor(
    message: string,
    options?: {
      readonly requeue?: boolean;
      readonly cause?: unknown;
    },
  ) {
    super(message);

    this.name =
      "QueueProcessingError";

    this.requeue =
      options?.requeue ?? false;

    if (
      options &&
      "cause" in options
    ) {
      this.cause =
        options.cause;
    }
  }
}