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
                , WORKFLOW_NAME='IC_CSNJ922B',WORKFLOW_ID='99051d55c28e4598a53a482a760fa4cf', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IC_CSNJ922B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 17, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('99051d55c28e4598a53a482a760fa4cf')

    csnj922bJob_vol = []
    csnj922bJob_volMnt = []
    csnj922bJob_env = [getICISConfigMap('icis-oder-infocomm-batch-configmap'), getICISConfigMap('icis-oder-infocomm-batch-configmap2'), getICISSecret('icis-oder-infocomm-batch-secret')]
    csnj922bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csnj922bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnj922bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'bca8b80aeae945509477701d5e5fb3a3',
        'volumes': csnj922bJob_vol,
        'volume_mounts': csnj922bJob_volMnt,
        'env_from':csnj922bJob_env,
        'task_id':'csnj922bJob',
        'image':'/icis/icis-oder-infocomm-batch:0.4.1.31',
        'arguments':["--job.name=csnj922bJob", "gszProcMonth="+str(datetime.now().strftime("%Y%m")), "requestDate=" + str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('99051d55c28e4598a53a482a760fa4cf')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnj922bJob,
        Complete
    ]) 

    # authCheck >> csnj922bJob >> Complete
    workflow








