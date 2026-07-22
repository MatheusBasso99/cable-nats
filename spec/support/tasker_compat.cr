# Crystal 1.21 replaced the legacy fiber scheduler with execution contexts,
# and the default context's scheduler rejects `spawn(same_thread: true)`,
# which Tasker — driving `Cable::BackendPinger` via `Tasker.every` — still
# passes. Re-open the timer to drop the flag: the default execution context
# resumes fibers with parallelism 1, so the original guarantee holds anyway.
class Timer
  def start_timer : Nil
    spawn { schedule_wait }
    Fiber.yield
  end
end
