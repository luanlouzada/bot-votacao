import asyncio
import json
import logging
import os
import random
import string
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from random import choice, uniform
from typing import Dict, List, Optional, Set

import aiohttp
import psutil

# Configuração avançada de logging para monitoramento detalhado
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(
            f'security_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        ),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


@dataclass
class DeviceProfile:
    """Simula diferentes perfis de dispositivos e navegadores"""

    platform: str
    browser: str
    version: str
    mobile: str
    device_id: str

    @staticmethod
    def generate_random():
        """Gera um perfil aleatório de dispositivo com características realistas"""
        platforms = ["Windows NT 10.0", "Macintosh", "Linux", "iPhone", "Android"]
        browsers = ["Chrome", "Firefox", "Safari", "Edge"]
        versions = ["131.0.6778.86", "120.0.2345.71", "119.0.3987.92"]
        mobiles = ["?0", "?1"]

        return DeviceProfile(
            platform=choice(platforms),
            browser=choice(browsers),
            version=choice(versions),
            mobile=choice(mobiles),
            device_id="".join(
                random.choices(string.ascii_letters + string.digits, k=16)
            ),
        )


class AsyncProxyRotator:
    """Gerenciador assíncrono de proxies com monitoramento avançado"""

    def __init__(self):
        self.proxies: Dict[str, Dict] = {}
        self.successful_proxies: Set[str] = set()
        self.blocked_proxies: List[Dict] = []
        self.min_success_rate = 0.3
        self.max_consecutive_failures = 2
        self.lock = asyncio.Lock()
        self.current_index = 0
        self.stats = {"total_tested": 0, "total_successful": 0, "total_failures": 0}
        self.blocked_proxy_cooldown = 60
        self.last_fetch_time = 0
        self.fetch_cooldown = 30

    async def add_to_blocked_queue(self, proxy: str):
        """Adiciona proxy à fila de bloqueados com timestamp"""
        async with self.lock:
            self.blocked_proxies.append(
                {"proxy": proxy, "blocked_time": datetime.now()}
            )
            logger.info(
                f"Proxy {proxy} adicionado à fila de bloqueados para retry posterior"
            )

    async def _test_proxy_quality(
        self, proxy: str, session: aiohttp.ClientSession
    ) -> Dict:
        """Teste assíncrono de qualidade do proxy com múltiplas métricas"""
        test_urls = ["http://httpbin.org/ip", "https://api.ipify.org?format=json"]
        results = {
            "success": 0,
            "total": len(test_urls),
            "avg_response_time": 0,
            "failures": 0,
            "last_success": None,
            "response_times": [],
        }

        for url in test_urls:
            try:
                start_time = asyncio.get_event_loop().time()
                async with session.get(
                    url,
                    proxy=f"http://{proxy}",
                    timeout=aiohttp.ClientTimeout(total=3),
                    ssl=False,  # Desabilita verificação SSL para testes
                ) as response:
                    if response.status == 200:
                        response_time = asyncio.get_event_loop().time() - start_time
                        results["success"] += 1
                        results["response_times"].append(response_time)
                        results["last_success"] = datetime.now()
            except:
                continue

        if results["response_times"]:
            results["avg_response_time"] = sum(results["response_times"]) / len(
                results["response_times"]
            )

        return results

    async def _fetch_proxies(self) -> List[str]:
        """Busca assíncrona de proxies de múltiplas fontes"""
        proxy_sources = [
            "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=5000&country=all&ssl=all&anonymity=elite",
            "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
            "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
            "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
            "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
        ]

        all_proxies = set()
        async with aiohttp.ClientSession() as session:
            tasks = []
            for source in proxy_sources:
                tasks.append(self._fetch_from_source(session, source))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for proxy_set in results:
                if isinstance(proxy_set, set):
                    all_proxies.update(proxy_set)

        return list(all_proxies)

    async def _fetch_from_source(
        self, session: aiohttp.ClientSession, source: str
    ) -> Set[str]:
        """Busca proxies de uma fonte específica com tratamento de erros"""
        try:
            async with session.get(
                source, timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status == 200:
                    text = await response.text()
                    proxies = {
                        line.strip()
                        for line in text.split("\n")
                        if line.strip() and ":" in line.strip()
                    }
                    logger.info(f"Obtidos {len(proxies)} proxies de {source}")
                    return proxies
        except Exception as e:
            logger.error(f"Erro ao obter proxies de {source}: {e}")
        return set()

    async def initialize_proxy_pool(
        self, min_proxies: int = 100, max_proxies: int = 2000
    ):
        current_time = time.time()
        if current_time - self.last_fetch_time < self.fetch_cooldown:
            await asyncio.sleep(
                self.fetch_cooldown - (current_time - self.last_fetch_time)
            )

        self.last_fetch_time = time.time()
        """Inicialização assíncrona do pool de proxies com limite de conexões"""
        all_proxies = await self._fetch_proxies()
        logger.info(f"Testando {len(all_proxies)} proxies...")

        # Limita o número de conexões simultâneas durante os testes
        semaphore = asyncio.Semaphore(200)
        async with aiohttp.ClientSession() as session:
            tasks = []
            for proxy in all_proxies:
                if len(self.proxies) >= max_proxies:
                    break
                tasks.append(self._test_and_add_proxy(proxy, session, semaphore))

            await asyncio.gather(*tasks)

        logger.info(
            f"Pool inicializado: {len(self.proxies)} proxies, "
            f"{len(self.successful_proxies)} bem sucedidos"
        )
        return len(self.proxies) >= min_proxies

    async def _test_and_add_proxy(
        self, proxy: str, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore
    ):
        """Testa e adiciona proxy ao pool com controle de concorrência"""
        async with semaphore:
            try:
                results = await self._test_proxy_quality(proxy, session)
                self.stats["total_tested"] += 1

                if results["success"] > 0:
                    async with self.lock:
                        self.proxies[proxy] = results
                        if (
                            results["success"] / results["total"]
                            >= self.min_success_rate
                        ):
                            self.successful_proxies.add(proxy)
                            self.stats["total_successful"] += 1
                            logger.info(
                                f"Proxy qualificado: {proxy} "
                                f"(Taxa: {results['success']/results['total']:.2%})"
                            )
            except Exception as e:
                self.stats["total_failures"] += 1
                logger.debug(f"Falha ao testar proxy {proxy}: {e}")

    async def get_next_proxy(self) -> Optional[str]:
        """Retorna próximo proxy, verificando também a fila de bloqueados"""
        async with self.lock:
            # Primeiro verifica se algum proxy bloqueado já pode ser reutilizado
            current_time = datetime.now()
            ready_proxies = [
                item["proxy"]
                for item in self.blocked_proxies
                if (current_time - item["blocked_time"]).total_seconds()
                >= self.blocked_proxy_cooldown
            ]

            # Remove os proxies prontos da fila de bloqueados e os readiciona ao pool
            if ready_proxies:
                self.blocked_proxies = [
                    item
                    for item in self.blocked_proxies
                    if item["proxy"] not in ready_proxies
                ]
                for proxy in ready_proxies:
                    if proxy not in self.proxies:
                        self.proxies[proxy] = {
                            "success": 0,
                            "failures": 0,
                            "last_success": None,
                        }
                logger.info(
                    f"{len(ready_proxies)} proxies reativados após período de cooldown"
                )

            # Tenta pegar um proxy bem-sucedido primeiro
            if self.successful_proxies:
                return random.choice(list(self.successful_proxies))

            # Se não houver proxies bem-sucedidos, usa qualquer um disponível
            if self.proxies:
                return random.choice(list(self.proxies.keys()))

            return None

    async def mark_proxy_result(
        self, proxy: str, success: bool, status_code: Optional[int] = None
    ):
        """Atualiza métricas do proxy, considerando código de status"""
        if not proxy:
            return

        async with self.lock:
            if proxy not in self.proxies and proxy not in [
                x["proxy"] for x in self.blocked_proxies
            ]:
                return

            if success:
                self.proxies[proxy]["failures"] = 0
                self.proxies[proxy]["last_success"] = datetime.now()
                self.successful_proxies.add(proxy)
            else:
                if status_code == 403:
                    # Move para a fila de bloqueados em vez de incrementar falhas
                    if proxy in self.proxies:
                        del self.proxies[proxy]
                    if proxy in self.successful_proxies:
                        self.successful_proxies.remove(proxy)
                    await self.add_to_blocked_queue(proxy)
                else:
                    # Para outros erros, incrementa contador de falhas normalmente
                    self.proxies[proxy]["failures"] += 1
                    if self.proxies[proxy]["failures"] >= self.max_consecutive_failures:
                        if proxy in self.successful_proxies:
                            self.successful_proxies.remove(proxy)
                        del self.proxies[proxy]
                        logger.warning(
                            f"Proxy removido após {self.max_consecutive_failures} falhas: {proxy}"
                        )

    async def get_stats(self) -> Dict:
        """Retorna estatísticas detalhadas do pool de proxies"""
        return {
            "active_proxies": len(self.proxies),
            "successful_proxies": len(self.successful_proxies),
            "blocked_proxies": len(self.blocked_proxies),
            "total_proxies": len(self.proxies) + len(self.blocked_proxies),
        }


class AsyncSecurityTester:
    """Sistema principal de teste de segurança usando operações assíncronas"""

    def __init__(self):
        self.proxy_rotator = AsyncProxyRotator()
        self.results = {
            "total_requests": 0,
            "successful": 0,
            "failed": 0,
            "rate_limited": 0,
            "blocked": 0,
            "proxy_errors": 0,
            "response_times": [],
            "errors": {},
            "proxy_stats": {},
        }
        self.rate_limit_delay = 0.2
        self.successful_proxies_usage = {}
        self.session: Optional[aiohttp.ClientSession] = None

    def _generate_headers(self, device: DeviceProfile) -> Dict:
        """Gera headers realistas para o dispositivo simulado"""
        return {
            "Host": "voting-api.globesoccer.com",
            "Sec-Ch-Ua-Platform": f'"{device.platform}"',
            "Accept-Language": "pt-BR,pt;q=0.9",
            "Sec-Ch-Ua": f'"{device.browser}";v="{device.version}", "Not_A Brand";v="24"',
            "Sec-Ch-Ua-Mobile": device.mobile,
            "User-Agent": f"Mozilla/5.0 ({device.platform}; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{device.version} Safari/537.36",
            "Accept": "*/*",
            "Origin": "https://vote.globesoccer.com",
            "Sec-Fetch-Site": "same-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://vote.globesoccer.com/",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }

    async def send_vote(
        self, voter_id: str, request_num: int, semaphore: asyncio.Semaphore
    ) -> None:
        """Envia voto de forma assíncrona com controle de concorrência"""
        async with semaphore:
            start_time = asyncio.get_event_loop().time()
            device = DeviceProfile.generate_random()
            current_proxy = None

            for attempt in range(3):
                proxy = await self.proxy_rotator.get_next_proxy()

                if not proxy:
                    logger.info("Lista de proxies esgotada, reiniciando pool...")
                    self.proxy_rotator.proxies.clear()
                    self.proxy_rotator.successful_proxies.clear()
                    self.proxy_rotator.blocked_proxies.clear()

                    await asyncio.sleep(5)

                    success = await self.proxy_rotator.initialize_proxy_pool(
                        min_proxies=10, max_proxies=2000
                    )

                    if not success:
                        logger.error("Falha ao reiniciar pool de proxies")
                        await asyncio.sleep(30)
                        continue

                    proxy = await self.proxy_rotator.get_next_proxy()
                    if not proxy:
                        continue

                try:
                    headers = self._generate_headers(device)
                    boundary = "----WebKitFormBoundaryVBRAdrHKA4pYcpJH"

                    data = (
                        f"--{boundary}\r\n"
                        f'Content-Disposition: form-data; name="eventCode"\r\n\r\nKLMEN24\r\n'
                        f"--{boundary}\r\n"
                        f'Content-Disposition: form-data; name="voterId"\r\n\r\n{voter_id}\r\n'
                        f"--{boundary}\r\n"
                        f'Content-Disposition: form-data; name="hash"\r\n\r\n\r\n'
                        f"--{boundary}\r\n"
                        f'Content-Disposition: form-data; name="vote"\r\n\r\n22\r\n'
                        f"--{boundary}--\r\n"
                    )

                    headers["Content-Type"] = (
                        f"multipart/form-data; boundary={boundary}"
                    )
                    headers["Content-Length"] = str(len(data))

                    async with self.session.post(
                        "https://voting-api.globesoccer.com/votes",
                        headers=headers,
                        data=data.encode(),
                        proxy=f"http://{proxy}",
                        timeout=aiohttp.ClientTimeout(total=30),
                        ssl=False,
                    ) as response:
                        response_time = asyncio.get_event_loop().time() - start_time
                        self.results["response_times"].append(response_time)

                        if response.status == 200:
                            self.results["successful"] += 1
                            await self.proxy_rotator.mark_proxy_result(proxy, True)
                            current_proxy = proxy
                            self.successful_proxies_usage[proxy] = (
                                self.successful_proxies_usage.get(proxy, 0) + 1
                            )
                            self.rate_limit_delay = max(
                                0.1, self.rate_limit_delay * 0.9
                            )
                            logger.info(
                                f"Vote {request_num}: Success - "
                                f"Proxy: {proxy} (Usos: {self.successful_proxies_usage[proxy]}) - "
                                f"Device: {device.device_id} - "
                                f"Time: {response_time:.2f}s"
                            )
                            return

                        elif response.status == 403:
                            self.results["blocked"] += 1
                            await self.proxy_rotator.mark_proxy_result(proxy, False)
                            logger.warning(
                                f"Vote {request_num}: Blocked (403) - "
                                f"Proxy: {proxy} - "
                                f"Device: {device.device_id}"
                            )
                            device = DeviceProfile.generate_random()
                            continue

                        elif response.status == 429:
                            self.results["rate_limited"] += 1
                            await self.proxy_rotator.mark_proxy_result(proxy, False)
                            self.rate_limit_delay *= 1.5
                            logger.warning(
                                f"Vote {request_num}: Rate limited - "
                                f"Proxy: {proxy} - "
                                f"Novo delay: {self.rate_limit_delay:.2f}s"
                            )
                            await asyncio.sleep(self.rate_limit_delay)
                            continue

                        else:
                            self.results["failed"] += 1
                            await self.proxy_rotator.mark_proxy_result(proxy, False)
                            error_key = f"status_{response.status}"
                            self.results["errors"][error_key] = (
                                self.results["errors"].get(error_key, 0) + 1
                            )
                            logger.error(
                                f"Vote {request_num}: Failed - "
                                f"Status: {response.status} - "
                                f"Proxy: {proxy}"
                            )

                            if attempt < 2:  # Máximo de 3 tentativas (0, 1, 2)
                                await asyncio.sleep(2**attempt + uniform(0, 1))

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    self.results["proxy_errors"] += 1
                    await self.proxy_rotator.mark_proxy_result(proxy, False)
                    logger.error(f"Proxy error with {proxy}: {str(e)}")
                    continue

                except Exception as e:
                    logger.error(
                        f"Vote {request_num}: Error - {str(e)} - Proxy: {proxy}"
                    )
                    if attempt < 2:
                        await asyncio.sleep(2**attempt + uniform(0, 1))

            self.results["failed"] += 1

    async def run_test(
        self,
        num_requests: int = 10000,
        max_concurrent: int = 500,
        batch_size: int = 1000,
    ):
        """Executa o teste em lotes com monitoramento de recursos"""
        logger.info(
            f"Iniciando teste com {num_requests} requisições, "
            f"{max_concurrent} conexões simultâneas"
        )

        self.results["start_time"] = datetime.now()
        semaphore = asyncio.Semaphore(max_concurrent)

        # Usa um único ClientSession para todo o teste
        async with aiohttp.ClientSession() as session:
            self.session = session

            # Processa em lotes para melhor gerenciamento de memória
            for batch_start in range(0, num_requests, batch_size):
                batch_end = min(batch_start + batch_size, num_requests)
                batch_size_actual = batch_end - batch_start

                # Cria tasks para o lote atual
                tasks = []
                for i in range(batch_start, batch_end):
                    self.results["total_requests"] += 1
                    voter_id = str(uuid.uuid4())
                    tasks.append(self.send_vote(voter_id, i + 1, semaphore))

                # Executa o lote atual
                await asyncio.gather(*tasks)

                # Monitora recursos e progresso
                memory_usage = (
                    psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
                )
                cpu_percent = psutil.cpu_percent()
                proxies_ativos = len(self.proxy_rotator.successful_proxies)
                taxa_sucesso = (
                    self.results["successful"] / self.results["total_requests"] * 100
                    if self.results["total_requests"] > 0
                    else 0
                )

                logger.info(
                    f"\nProgresso do Teste:\n"
                    f"Completado: {batch_end}/{num_requests} requisições\n"
                    f"Memória em uso: {memory_usage:.1f}MB\n"
                    f"CPU: {cpu_percent}%\n"
                    f"Proxies ativos: {proxies_ativos}\n"
                    f"Taxa de sucesso: {taxa_sucesso:.1f}%\n"
                )

        self.results["end_time"] = datetime.now()
        await self.generate_report()

    async def generate_report(self):
        """Gera relatório detalhado do teste"""
        duration = (
            self.results["end_time"] - self.results["start_time"]
        ).total_seconds()

        report = {
            "duração_teste": f"{duration:.2f} segundos",
            "total_requisições": self.results["total_requests"],
            "sucessos": self.results["successful"],
            "falhas": self.results["failed"],
            "bloqueios": self.results["blocked"],
            "erros_proxy": self.results["proxy_errors"],
            "rate_limited": self.results["rate_limited"],
            "taxa_sucesso": f"{(self.results['successful'] / self.results['total_requests'] * 100):.2f}%",
            "taxa_bloqueio": f"{(self.results['blocked'] / self.results['total_requests'] * 100):.2f}%",
            "erros_por_tipo": self.results["errors"],
            "estatísticas_proxies": {
                "total_proxies": len(self.proxy_rotator.proxies),
                "proxies_bem_sucedidos": len(self.proxy_rotator.successful_proxies),
                "uso_por_proxy": self.successful_proxies_usage,
            },
        }

        if self.results["response_times"]:
            report["tempos_resposta"] = {
                "mínimo": f"{min(self.results['response_times']):.3f}s",
                "máximo": f"{max(self.results['response_times']):.3f}s",
                "médio": f"{sum(self.results['response_times']) / len(self.results['response_times']):.3f}s",
            }

        report_path = f'security_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        logger.info(f"Relatório gerado: {report_path}")
        logger.info("Resumo do teste:")
        logger.info(json.dumps(report, ensure_ascii=False, indent=2))


async def main():
    """Função principal com configuração automática de recursos em loop infinito"""
    while True:
        try:
            cpu_count = os.cpu_count() or 1
            memory_gb = psutil.virtual_memory().total / (1024 * 1024 * 1024)

            max_concurrent = min(
                int(cpu_count * 8),
                int(memory_gb * 100),
                2000,
            )

            logger.info(
                f"Configuração do Sistema:\n"
                f"CPU Cores: {cpu_count}\n"
                f"Memória Total: {memory_gb:.1f}GB\n"
                f"Conexões simultâneas: {max_concurrent}"
            )

            tester = AsyncSecurityTester()

            if not await tester.proxy_rotator.initialize_proxy_pool(
                min_proxies=10, max_proxies=2000
            ):
                logger.error(
                    "Não foi possível obter proxies suficientes. Tentando novamente..."
                )
                await asyncio.sleep(60)
                continue

            await tester.run_test(
                num_requests=1000000,
                max_concurrent=max_concurrent,
                batch_size=5000,
            )

            logger.info("Ciclo completo, iniciando novo ciclo...")
            await asyncio.sleep(5)  # Small pause between cycles

        except KeyboardInterrupt:
            logger.info("Teste interrompido pelo usuário")
            break
        except Exception as e:
            logger.error(f"Erro durante o teste: {str(e)}")
            await asyncio.sleep(30)
            continue


if __name__ == "__main__":
    # Configura uvloop se disponível para melhor performance
    try:
        import uvloop

        uvloop.install()
        logger.info("uvloop instalado para melhor performance")
    except ImportError:
        logger.info("uvloop não disponível, usando asyncio padrão")

    asyncio.run(main())
