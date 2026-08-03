export type Upcaster = (payload: Readonly<unknown>) => Readonly<unknown>;

export class UnknownEventVersionError extends Error {
  readonly code = "unknown_event_version";
}

export class UpcasterRegistry {
  readonly #chains = new Map<string, Map<number, Upcaster>>();
  readonly #current = new Map<string, number>();

  register(eventName: string, fromVersion: number, upcaster: Upcaster): void {
    if (!Number.isInteger(fromVersion) || fromVersion < 1)
      throw new TypeError("invalid source version");
    const chain = this.#chains.get(eventName) ?? new Map<number, Upcaster>();
    if (chain.has(fromVersion))
      throw new Error(`duplicate upcaster: ${eventName}@${fromVersion}`);
    chain.set(fromVersion, upcaster);
    this.#chains.set(eventName, chain);
  }

  setCurrentVersion(eventName: string, version: number): void {
    if (!Number.isInteger(version) || version < 1)
      throw new TypeError("invalid current version");
    this.#current.set(eventName, version);
  }

  upcast(
    eventName: string,
    version: number,
    payload: Readonly<unknown>,
  ): { version: number; payload: Readonly<unknown> } {
    const current = this.#current.get(eventName);
    if (current === undefined || version > current || version < 1)
      throw new UnknownEventVersionError(`${eventName}@${version}`);
    const chain = this.#chains.get(eventName) ?? new Map<number, Upcaster>();
    let output = payload;
    for (let source = version; source < current; source += 1) {
      const upcaster = chain.get(source);
      if (upcaster === undefined)
        throw new UnknownEventVersionError(`${eventName}@${source}`);
      output = upcaster(output);
    }
    return { version: current, payload: output };
  }
}
