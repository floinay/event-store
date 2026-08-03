import type { StoredEvent } from "@event-store/contracts";

export type Decide<State, Command, Event> = (
  state: Readonly<State>,
  command: Readonly<Command>,
) => readonly Event[];
export type Evolve<State, Event> = (
  state: Readonly<State>,
  event: Readonly<Event>,
) => State;
export type Upcaster = (storedPayload: Readonly<unknown>) => Readonly<unknown>;

export interface AggregateDefinition<State, DomainEvent> {
  initial: State;
  evolve: Evolve<State, DomainEvent>;
  decode: (event: StoredEvent) => DomainEvent;
}

export interface LoadedAggregate<State> {
  state: State;
  revision: bigint;
}

export function fold<State, Event>(
  initial: State,
  evolve: Evolve<State, Event>,
  events: readonly Event[],
): State {
  return events.reduce((state, event) => evolve(state, event), initial);
}

export function reconstruct<State, DomainEvent>(
  definition: AggregateDefinition<State, DomainEvent>,
  events: readonly StoredEvent[],
  snapshot?: { state: State; revision: bigint },
): LoadedAggregate<State> {
  let expected = snapshot?.revision ?? 0n;
  let state = snapshot?.state ?? definition.initial;
  for (const stored of events) {
    const revision = BigInt(stored.streamRevision);
    if (revision !== expected + 1n)
      throw new Error(`event_gap: expected ${expected + 1n}, got ${revision}`);
    state = definition.evolve(state, definition.decode(stored));
    expected = revision;
  }
  return { state, revision: expected };
}
