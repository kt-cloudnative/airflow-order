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
                , WORKFLOW_NAME='IA_CBOT957B',WORKFLOW_ID='d2c1a44955164fbc93d454c1d63ab3bf', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IA_CBOT957B-0.0.prd-tz.0.0'
    ,'schedule_interval':'0 21 * * *'
    ,'start_date': datetime(2024, 7, 15, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('d2c1a44955164fbc93d454c1d63ab3bf')

    cbot957bJob_vol = []
    cbot957bJob_volMnt = []
    cbot957bJob_env = [getICISConfigMap('icis-oder-inetaplca-batch-configmap'), getICISConfigMap('icis-oder-inetaplca-batch-configmap2'), getICISSecret('icis-oder-inetaplca-batch-secret')]
    cbot957bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbot957bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot957bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'd8452b3a19674595af3fd87a8cf5f370',
        'volumes': cbot957bJob_vol,
        'volume_mounts': cbot957bJob_volMnt,
        'env_from':cbot957bJob_env,
        'task_id':'cbot957bJob',
        'image':'/icis/icis-oder-inetaplca-batch:0.4.1.26',
        'arguments':["--job.name=cbot957bJob", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('d2c1a44955164fbc93d454c1d63ab3bf')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot957bJob,
        Complete
    ]) 

    # authCheck >> cbot957bJob >> Complete
    workflow








