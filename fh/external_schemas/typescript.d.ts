/**
 * TypeScript Type Definitions for Fund Holdings API
 * 
 * These types define the structure of JSON responses from the GetFundHoldings R2 storage.
 * Use these types in your frontend applications for type safety and better developer experience.
 * 
 * Generated from: fh/api_schemas/json_schema.json
 * Last updated: 2025-07-11
 */

/**
 * Metadata about the holdings dataset
 */
export interface HoldingsMetadata {
  /** Fund ticker symbol (e.g., 'IVV', 'VOO') */
  fund_ticker: string;
  
  /** SEC CIK identifier for the fund company */
  cik: string;
  
  /** Total number of holdings in the dataset */
  total_holdings: number;
  
  /** Processing timestamp in YYYYMMDD_HHMMSS format */
  data_timestamp: string;
  
  /** Original CSV filename from the data pipeline */
  source_file?: string;
  
  /** ISO 8601 timestamp when data was uploaded to R2 */
  upload_timestamp: string;
}

/**
 * Individual security holding information
 */
export interface Holding {
  /** Security name */
  name: string;
  
  /** Legal Entity Identifier */
  lei: string | null;
  
  /** Security title/description */
  title: string;
  
  /** CUSIP identifier (9 characters) */
  cusip: string | null;
  
  /** ISIN identifier (12 characters) */
  isin: string | null;
  
  /** Other identifier value */
  other_id: string | null;
  
  /** Other identifier description */
  other_id_desc: string | null;
  
  /** Security balance/shares */
  balance: number;
  
  /** Units of measurement (e.g., 'NS' for number of shares) */
  units: string;
  
  /** Currency code (typically 'USD') */
  currency: string;
  
  /** Market value in USD (can be negative for short positions) */
  value_usd: number;
  
  /** Percentage of total portfolio value (decimal, e.g., 0.025 for 2.5%) */
  percent_value: number;
  
  /** Payoff profile (e.g., 'Long', 'Short') */
  payoff_profile: string;
  
  /** Asset category code */
  asset_category: string;
  
  /** Issuer category code */
  issuer_category: string;
  
  /** Investment country code (ISO 3166-1 alpha-2) */
  investment_country: string;
  
  /** Restricted security flag ('Y' or 'N') */
  is_restricted_security: 'Y' | 'N' | '';
  
  /** Fair value level (1, 2, or 3) */
  fair_value_level: string;
  
  /** Cash collateral flag ('Y' or 'N') */
  is_cash_collateral: 'Y' | 'N' | '';
  
  /** Non-cash collateral flag ('Y' or 'N') */
  is_non_cash_collateral: 'Y' | 'N' | '';
  
  /** Loan by fund flag ('Y' or 'N') */
  is_loan_by_fund: 'Y' | 'N' | '';
  
  /** Loan value if applicable */
  loan_value: number | null;
  
  /** Source N-PORT XML filename */
  source_file: string;
  
  /** N-PORT report period date (YYYY-MM-DD) */
  report_period_date: string;
  
  /** Ticker symbol from OpenFIGI API enrichment */
  ticker: string | null;
  
  /** ISO 8601 timestamp when ticker enrichment was performed (UTC) */
  enrichment_datetime: string;
  
  /** Fund ticker symbol (same as metadata.fund_ticker) */
  fund_ticker: string;
  
  /** SEC series identifier */
  series_id: string;
}

/**
 * Complete fund holdings response structure
 */
export interface HoldingsResponse {
  /** Metadata about the holdings dataset */
  metadata: HoldingsMetadata;
  
  /** Array of individual security holdings */
  holdings: Holding[];
}

/**
 * Type guard to check if an object is a valid HoldingsResponse
 */
export function isHoldingsResponse(obj: any): obj is HoldingsResponse {
  return (
    obj &&
    typeof obj === 'object' &&
    obj.metadata &&
    typeof obj.metadata.fund_ticker === 'string' &&
    typeof obj.metadata.cik === 'string' &&
    typeof obj.metadata.total_holdings === 'number' &&
    Array.isArray(obj.holdings)
  );
}

/**
 * Type guard to check if an object is a valid Holding
 */
export function isHolding(obj: any): obj is Holding {
  return (
    obj &&
    typeof obj === 'object' &&
    typeof obj.name === 'string' &&
    typeof obj.title === 'string' &&
    typeof obj.balance === 'number' &&
    typeof obj.value_usd === 'number' &&
    typeof obj.percent_value === 'number'
  );
}

/**
 * Utility types for specific use cases
 */

/** Holdings sorted by value (descending) */
export type HoldingsByValue = Holding[];

/** Holdings grouped by asset category */
export type HoldingsByCategory = Record<string, Holding[]>;

/** Holdings with successful ticker enrichment only */
export type EnrichedHoldings = (Holding & { ticker: string })[];

/** Summary statistics for a holdings dataset */
export interface HoldingsSummary {
  total_holdings: number;
  total_value_usd: number;
  top_10_holdings: Holding[];
  asset_categories: string[];
  countries: string[];
  enrichment_rate: number; // Percentage of holdings with tickers
}

/**
 * API response wrapper for error handling
 */
export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  error?: string;
  timestamp: string;
}

/**
 * Specific API response for holdings
 */
export type HoldingsApiResponse = ApiResponse<HoldingsResponse>;

/**
 * Enum-like constants for common values
 */
export const PAYOFF_PROFILES = {
  LONG: 'Long',
  SHORT: 'Short'
} as const;

export const FLAG_VALUES = {
  YES: 'Y',
  NO: 'N',
  EMPTY: ''
} as const;

export const COMMON_CURRENCIES = {
  USD: 'USD',
  EUR: 'EUR',
  GBP: 'GBP',
  JPY: 'JPY'
} as const;

/**
 * Helper functions for working with holdings data
 */
export namespace HoldingsUtils {
  /**
   * Calculate total portfolio value from holdings array
   */
  export function getTotalValue(holdings: Holding[]): number {
    return holdings.reduce((sum, holding) => sum + holding.value_usd, 0);
  }
  
  /**
   * Get top N holdings by value
   */
  export function getTopHoldings(holdings: Holding[], count: number = 10): Holding[] {
    return [...holdings]
      .sort((a, b) => b.value_usd - a.value_usd)
      .slice(0, count);
  }
  
  /**
   * Group holdings by asset category
   */
  export function groupByCategory(holdings: Holding[]): HoldingsByCategory {
    return holdings.reduce((groups, holding) => {
      const category = holding.asset_category;
      if (!groups[category]) {
        groups[category] = [];
      }
      groups[category].push(holding);
      return groups;
    }, {} as HoldingsByCategory);
  }
  
  /**
   * Filter holdings that have ticker symbols
   */
  export function getEnrichedHoldings(holdings: Holding[]): EnrichedHoldings {
    return holdings.filter((h): h is Holding & { ticker: string } => 
      h.ticker !== null && h.ticker !== ''
    );
  }
  
  /**
   * Calculate enrichment rate (percentage of holdings with tickers)
   */
  export function getEnrichmentRate(holdings: Holding[]): number {
    const enriched = getEnrichedHoldings(holdings);
    return holdings.length > 0 ? (enriched.length / holdings.length) * 100 : 0;
  }
}