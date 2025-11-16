<img width="1098" height="230" alt="image" src="https://github.com/user-attachments/assets/afbf79eb-85c6-4722-bbce-50fa622cd020" />


# Pacifica 순차 전략 봇

Pacifica 거래소에서 BTC 숏 포지션을 자동으로 진입/청산하는 순차 실행 봇입니다.

## 주요 기능

- **순차 전략**: ENTER_SHORT → HOLD → EXIT_SHORT 순환
- **자동 재주문**: 5초마다 최적 가격으로 주문 갱신
- **포지션 관리**: 시작 시 기존 포지션 자동 정리 (FLUSH)
- **ROI 목표**: 설정된 수익률 도달 시 자동 청산
- **실시간 상태 보드**: Rich UI 또는 텍스트 기반 모니터링

## 설치

```bash
pip install -r requirements.txt
```

## 환경 변수 설정

`.env` 파일을 생성하고 다음 내용을 설정하세요:

```bash
# 필수 설정
PF_PRIVATE_KEY_B58=your_private_key_here
PF_API_KEY=보통 내 지갑주소넣음
PF_ENV=mainnet                  # mainnet 또는 testnet
PF_SYMBOL=BTC                   # 거래 심볼

# 거래 설정
PF_NOTIONAL_USDC=20            # 주문 금액 (USDC)
PF_REPRICE_SEC=5               # 재주문 간격 (초)

# 홀딩 시간
PF_HOLD_MIN_SEC=300            # 최소 홀드 시간 (초)
PF_HOLD_MAX_SEC=600            # 최대 홀드 시간 (초)

# 수익률 설정
PF_TARGET_ROI_PCT=1.0          # 목표 수익률 (%)
PF_USE_ROI=1                   # ROI 체크 활성화 (1=사용, 0=미사용)

# 시작 옵션
PF_FLUSH_ON_START=1            # 시작 시 포지션 정리
PF_FLUSH_WAIT_SEC=120          # 정리 대기 시간 (초)
PF_AVOID_DOUBLE_ENTRY=1        # 중복 진입 방지

# API 제한
PF_POS_REST_MIN_SEC=30         # 포지션 조회 최소 간격
PF_POS_REST_429_SEC=180        # Rate limit 쿨다운
PF_POS_REST_ERR_SEC=60         # 에러 시 쿨다운
```

## 실행

```bash
python trade.py
```

## 상태 설명

- **INIT**: 초기화 및 포지션 체크
- **FLUSH**: 기존 포지션 정리 중
- **ENTER_SHORT**: 숏 포지션 진입 중
- **HOLD**: 포지션 홀딩 중 (수익률/시간 대기)
- **EXIT_SHORT**: 포지션 청산 중

## 주의사항

⚠️ **실제 자금을 사용하기 전 테스트넷에서 충분히 테스트하세요**

- 시작 시 기존 포지션이 있으면 자동으로 FLUSH 모드로 전환
- ROI 또는 홀딩 시간 중 먼저 조건이 충족되면 EXIT
- WebSocket 연결 끊김 시 자동 재연결
- 재주문 간격은 최소 5초 권장

## 로그

봇은 다음 정보를 실시간으로 출력합니다:
- 현재 상태 (STATE)
- 포지션 정보 (side, amount)
- 진입 가격 및 ROI
- 주문 상태 (entry/exit orders)
- 오더북 정보 (bid/ask)
- 다음 재주문까지 시간

## 문제 해결

### "PF_PRIVATE_KEY_B58 필요" 에러
→ `.env` 파일에 개인키를 설정하세요

### 포지션 정리가 안 됨
→ `PF_FLUSH_WAIT_SEC` 시간을 늘리거나 수동으로 포지션을 청산하세요

### API Rate Limit 에러
→ `PF_POS_REST_MIN_SEC` 값을 늘리세요 (예: 60)

## 라이선스

MIT
