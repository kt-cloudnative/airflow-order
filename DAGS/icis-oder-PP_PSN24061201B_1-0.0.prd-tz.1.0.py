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
                , WORKFLOW_NAME='PP_PSN24061201B_1',WORKFLOW_ID='73957f543890426dac00bf1e6d1c2a2a', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_PSN24061201B_1-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 30, 15, 11, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('73957f543890426dac00bf1e6d1c2a2a')

    psn24061201bJob_vol = []
    psn24061201bJob_volMnt = []
    psn24061201bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    psn24061201bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    psn24061201bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    psn24061201bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'fb662cab78ed48c9966bfdb37541170d',
        'volumes': psn24061201bJob_vol,
        'volume_mounts': psn24061201bJob_volMnt,
        'env_from':psn24061201bJob_env,
        'task_id':'psn24061201bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=psn24061201bJob", "wrkDivCd=1"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('73957f543890426dac00bf1e6d1c2a2a')

    workflow = COMMON.getICISPipeline([
        authCheck,
        psn24061201bJob,
        Complete
    ]) 

    # authCheck >> psn24061201bJob >> Complete
    workflow








