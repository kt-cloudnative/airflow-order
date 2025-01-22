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
                , WORKFLOW_NAME='IA_CBOT963B',WORKFLOW_ID='917c1a609c4647589e9c1e880cca5d39', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IA_CBOT963B-0.2.prd-tz.0.0'
    ,'schedule_interval':'20 20 * * *'
    ,'start_date': datetime(2024, 7, 16, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('917c1a609c4647589e9c1e880cca5d39')

    cbot963bJob_vol = []
    cbot963bJob_volMnt = []
    cbot963bJob_env = [getICISConfigMap('icis-oder-inetaplca-batch-configmap'), getICISConfigMap('icis-oder-inetaplca-batch-configmap2'), getICISSecret('icis-oder-inetaplca-batch-secret')]
    cbot963bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbot963bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot963bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'ff649848074049fa936bbaafe89407a9',
        'volumes': cbot963bJob_vol,
        'volume_mounts': cbot963bJob_volMnt,
        'env_from':cbot963bJob_env,
        'task_id':'cbot963bJob',
        'image':'/icis/icis-oder-inetaplca-batch:0.4.1.24',
        'arguments':["--job.name=cbot963bJob", "requestDate="+str(datetime.now()), "procDate="+str(datetime.now().strftime("%Y%m%d"))],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('917c1a609c4647589e9c1e880cca5d39')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot963bJob,
        Complete
    ]) 

    # authCheck >> cbot963bJob >> Complete
    workflow








