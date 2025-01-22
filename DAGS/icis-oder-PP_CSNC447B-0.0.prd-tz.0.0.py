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
                , WORKFLOW_NAME='PP_CSNC447B',WORKFLOW_ID='c4fdcbacf3d245f2b83a576d6660e6dd', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNC447B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 3, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('c4fdcbacf3d245f2b83a576d6660e6dd')

    csnc447bJob_vol = []
    csnc447bJob_volMnt = []
    csnc447bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnc447bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csnc447bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnc447bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'd3ec4f86454745628dcb2d7f74a5aac3',
        'volumes': csnc447bJob_vol,
        'volume_mounts': csnc447bJob_volMnt,
        'env_from':csnc447bJob_env,
        'task_id':'csnc447bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.107',
        'arguments':["--job.name=csnc447bJob", "requestDate="+str(datetime.now()), "workDate=201906", "saDtlCd=2342", "dcRate=1000"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('c4fdcbacf3d245f2b83a576d6660e6dd')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnc447bJob,
        Complete
    ]) 

    # authCheck >> csnc447bJob >> Complete
    workflow








