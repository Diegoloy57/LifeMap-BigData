
pipeline {

    agent any

    options {
        skipDefaultCheckout(true)
        timestamps()
        disableConcurrentBuilds()
    }

    environment {
        COMPOSE_PROJECT_NAME = 'lifemap'
        DOCKER_BUILDKIT = '1'
    }

    stages {

        stage('1. Checkout') {
            steps {
                echo 'Descargando LifeMap desde GitHub...'
                checkout scm

                script {
                    env.DEPLOY_COMMIT = sh(
                        script: 'git rev-parse --short HEAD',
                        returnStdout: true
                    ).trim()
                }

                echo "Commit: ${env.DEPLOY_COMMIT}"
            }
        }

        stage('2. Validar Docker') {
            steps {
                sh '''
                    docker --version
                    docker compose version
                    docker compose config --quiet
                '''
            }
        }

        stage('3. Construir API') {
            steps {
                echo 'Construyendo Flask...'

                sh '''
                    docker compose build api
                '''
            }
        }

        stage('4. Validar sintaxis') {
            steps {
                echo 'Validando archivos Python...'

                sh '''
                    docker compose run --rm --no-deps \
                        --entrypoint python api \
                        -m compileall -q \
                        api dask spark scripts
                '''
            }
        }

        stage('5. Pruebas automatizadas') {
            steps {
                echo 'Ejecutando pruebas de LifeMap...'

                sh '''
                docker compose run --rm --no-deps \
                --entrypoint python api \
                -c "import unittest, sys; suite = unittest.defaultTestLoader.discover('tests', pattern='test_lifemap_api.py'); count = suite.countTestCases(); print('Pruebas encontradas:', count); sys.exit(1) if count < 5 else None; result = unittest.TextTestRunner(verbosity=2).run(suite); sys.exit(0 if result.wasSuccessful() else 1)"
                '''
            }
        }

        stage('6. Iniciar MongoDB') {
            steps {
                echo 'Iniciando MongoDB LifeMap...'

                sh '''
                    docker compose up -d --wait mongo
                '''
            }
        }

        stage('7. Desplegar API') {
            steps {
                echo 'Desplegando Flask LifeMap...'

                withEnv(["GIT_COMMIT=${env.DEPLOY_COMMIT}"]) {
                    sh '''
                        echo "Commit que se desplegará: $GIT_COMMIT"
                        docker compose config --format json | python3 -c "import json,sys; d=json.load(sys.stdin); print('Commit en Compose:', d['services']['api']['environment'].get('GIT_COMMIT'))"
                        docker compose up -d --no-deps --force-recreate api

                    '''
                }
            }
        }

        stage('8. Smoke test') {
            steps {
                echo 'Verificando API y MongoDB...'

                sh '''
                    docker exec lifemap-api python -c '
import json
import time
import urllib.request

url = "http://127.0.0.1:5000/health"

for attempt in range(15):
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            data = json.load(response)

        assert data["status"] == "ok"
        assert data["database"]["mongodb"] == "connected"
        assert data["database"]["businesses_estimated"] > 0

        print("LifeMap API: OK")
        print("MongoDB: OK")
        print("Negocios:", data["database"]["businesses_estimated"])
        print("Commit:", data["git_commit"])
        break

    except Exception:
        if attempt == 14:
            raise
        time.sleep(2)
'
                '''
            }
        }
    }

    post {

        success {
            echo '========================================'
            echo '       LIFEMAP CI/CD EXITOSO'
            echo '========================================'
            echo 'Checkout : OK'
            echo 'Sintaxis : OK'
            echo 'Tests    : OK'
            echo 'MongoDB  : OK'
            echo 'Flask    : DEPLOYED'
            echo '========================================'
        }

        failure {
            echo '========================================'
            echo '        LIFEMAP CI/CD FALLÓ'
            echo '========================================'
            echo 'Revisar la etapa que presentó el error.'
            echo '========================================'
        }
    }
}
