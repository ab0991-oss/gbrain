import type { StructuredCommitment, WhatsAppMessage } from './extractor.ts';

export interface StoreQualifiedWhatsAppMessage extends WhatsAppMessage {
  store_key?: string | null;
  store_path?: string | null;
}

export interface SourceMessageIndex {
  byIdentity: Map<string, StoreQualifiedWhatsAppMessage>;
  byBareId: Map<string, StoreQualifiedWhatsAppMessage[]>;
  singleMessage: StoreQualifiedWhatsAppMessage | null;
}

const STORE_MESSAGE_ID_DELIMITER = '::';

export function buildSourceMessageRef(message: StoreQualifiedWhatsAppMessage): string {
  const msgId = asOptionalNonEmptyString(message.MsgID);
  if (!msgId) {
    return '';
  }

  const storeKey = asOptionalNonEmptyString(message.store_key);
  if (!storeKey) {
    return msgId;
  }

  return `${storeKey}${STORE_MESSAGE_ID_DELIMITER}${msgId}`;
}

export function describeSourceMessageIdContract(): string {
  return `Exact source_message_id from the source message. Use ${STORE_MESSAGE_ID_DELIMITER}-qualified form (${`store_key${STORE_MESSAGE_ID_DELIMITER}MsgID`}) when store_key is present; otherwise use bare MsgID.`;
}

export function buildSourceMessageIndex(
  messages: StoreQualifiedWhatsAppMessage[]
): SourceMessageIndex {
  const byIdentity = new Map<string, StoreQualifiedWhatsAppMessage>();
  const byBareId = new Map<string, StoreQualifiedWhatsAppMessage[]>();

  for (const message of messages) {
    const identity = buildSourceMessageRef(message);
    if (identity) {
      byIdentity.set(identity, message);
    }

    const bareId = asOptionalNonEmptyString(message.MsgID);
    if (!bareId) {
      continue;
    }

    const bucket = byBareId.get(bareId);
    if (bucket) {
      bucket.push(message);
    } else {
      byBareId.set(bareId, [message]);
    }
  }

  return {
    byIdentity,
    byBareId,
    singleMessage: messages.length === 1 ? messages[0] : null,
  };
}

export function resolveSourceMessage(
  messages: StoreQualifiedWhatsAppMessage[],
  commitment: StructuredCommitment,
  sourceIndex?: SourceMessageIndex
): StoreQualifiedWhatsAppMessage | null {
  const index = sourceIndex ?? buildSourceMessageIndex(messages);
  if (messages.length === 0) {
    return null;
  }

  const explicitSourceMessageId = asOptionalNonEmptyString(commitment.source_message_id);
  if (explicitSourceMessageId) {
    const exactIdentityMatch = index.byIdentity.get(explicitSourceMessageId);
    if (exactIdentityMatch) {
      return exactIdentityMatch;
    }

    const bareMatches = index.byBareId.get(explicitSourceMessageId) ?? [];
    if (bareMatches.length === 1) {
      return bareMatches[0];
    }

    if (bareMatches.length > 1) {
      return null;
    }
  }

  return index.singleMessage;
}

export function resolveSourceMessageId(
  messages: StoreQualifiedWhatsAppMessage[],
  commitment: StructuredCommitment,
  message: StoreQualifiedWhatsAppMessage | null,
  sourceIndex?: SourceMessageIndex
): string | null {
  const index = sourceIndex ?? buildSourceMessageIndex(messages);
  if (message) {
    return buildSourceMessageRef(message);
  }

  if (messages.length === 0) {
    return asOptionalNonEmptyString(commitment.source_message_id);
  }

  const explicitSourceMessageId = asOptionalNonEmptyString(commitment.source_message_id);
  if (!explicitSourceMessageId) {
    return null;
  }

  const exactIdentityMatch = index.byIdentity.get(explicitSourceMessageId);
  if (exactIdentityMatch) {
    return buildSourceMessageRef(exactIdentityMatch);
  }

  const bareMessageId = getBareMessageId(explicitSourceMessageId);
  const bareMatches = index.byBareId.get(bareMessageId) ?? [];
  if (bareMatches.length === 1) {
    return buildSourceMessageRef(bareMatches[0]);
  }

  if (bareMatches.length > 1) {
    throw new Error(
      `Ambiguous source_message_id: ${explicitSourceMessageId} matches multiple store-qualified messages in this batch.`
    );
  }

  return null;
}

function getBareMessageId(sourceMessageId: string): string {
  const delimiterIndex = sourceMessageId.indexOf(STORE_MESSAGE_ID_DELIMITER);
  if (delimiterIndex === -1) {
    return sourceMessageId;
  }

  return sourceMessageId.slice(delimiterIndex + STORE_MESSAGE_ID_DELIMITER.length);
}

function asOptionalNonEmptyString(value: unknown): string | null {
  if (typeof value !== 'string') {
    return null;
  }

  const normalized = value.trim();
  return normalized.length > 0 ? normalized : null;
}
