from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='dydwlsxptmxm',WORKFLOW_ID='f4adc1111792496d9a0bdbc8dfe3c6c2', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-dydwlsxptmxm-1.0.prd-tz.0.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 14, 18, 51, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('f4adc1111792496d9a0bdbc8dfe3c6c2')

    dydwls_task_id_vol = []
    dydwls_task_id_volMnt = []
    dydwls_task_id_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    dydwls_task_id_env.extend([getICISConfigMap('icis-oder-pvctest-mng-configmap'), getICISSecret('icis-oder-pvctest-mng-secret'), getICISConfigMap('icis-oder-pvctest-configmap'), getICISSecret('icis-oder-pvctest-secret')])
    dydwls_task_id_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    dydwls_task_id = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '9ae9da489d9642648edf4b90663182d3',
        'volumes': dydwls_task_id_vol,
        'volume_mounts': dydwls_task_id_volMnt,
        'env_from':dydwls_task_id_env,
        'task_id':'dydwls_task_id',
        'image':'/icis/icis-oder-pvctest:1.1.0.1',
        'arguments':[argu],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      


    another_task_id_vol = []
    another_task_id_volMnt = []
    another_task_id_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    another_task_id_env.extend([getICISConfigMap('icis-oder-pvctest-mng-configmap'), getICISSecret('icis-oder-pvctest-mng-secret'), getICISConfigMap('icis-oder-pvctest-configmap'), getICISSecret('icis-oder-pvctest-secret')])
    another_task_id_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    another_task_id = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'ef5b0f2aab9141cbaa8f0b6a2ad1d432',
        'volumes': another_task_id_vol,
        'volume_mounts': another_task_id_volMnt,
        'env_from':another_task_id_env,
        'task_id':'another_task_id',
        'image':'/icis/icis-oder-pvctest:1.1.0.1',
        'arguments':[another],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      


    Complete = COMMON.getICISCompleteWflowTask('f4adc1111792496d9a0bdbc8dfe3c6c2')

    workflow = COMMON.getICISPipeline([
        authCheck,
        dydwls_task_id,
        another_task_id,
        Complete
    ]) 

    # authCheck >> dydwls_task_id >> another_task_id >> Complete
    workflow








