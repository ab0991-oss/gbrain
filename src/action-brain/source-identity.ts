import type { StructuredCommitment, WhatsAppMessage } from './extractor.ts';

export interface StoreQualifiedWhatsAppMessage extends WhatsAppMessage {
  store_key?: string | null;
  store_path?: string | null;
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

export function resolveSourceMessage(
  messages: StoreQualifiedWhatsAppMessage[],
  commitment: StructuredCommitment
): StoreQualifiedWhatsAppMessage | null {
  if (messages.length === 0) {
    return null;
  }

  const explicitSourceMessageId = asOptionalNonEmptyString(commitment.source_message_id);
  if (explicitSourceMessageId) {
    const exactIdentityMatch = messages.find((message) => buildSourceMessageRef(message) === explicitSourceMessageId);
    if (exactIdentityMatch) {
      return exactIdentityMatch;
    }

    const bareMatches = messages.filter((message) => message.MsgID === explicitSourceMessageId);
    if (bareMatches.length === 1) {
      return bareMatches[0];
    }

    if (bareMatches.length > 1) {
      return null;
    }
  }

  return messages.length === 1 ? messages[0] : null;
}

export function resolveSourceMessageId(
  messages: StoreQualifiedWhatsAppMessage[],
  commitment: StructuredCommitment,
  message: StoreQualifiedWhatsAppMessage | null
): string | null {
  if (message) {
    return buildSourceMessageRef(message);
  }

  if (messages.length === 0) {
    return asOptionalNonEmptyString(commitment.source_message_id);
  }

  return null;
}

function asOptionalNonEmptyString(value: unknown): string | null {
  if (typeof value !== 'string') {
    return null;
  }

  const normalized = value.trim();
  return normalized.length > 0 ? normalized : null;
}
