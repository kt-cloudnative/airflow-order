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
                , WORKFLOW_NAME='BI_CSAB911B',WORKFLOW_ID='7cf3e40577314028b68d7ebcefa85bfd', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-BI_CSAB911B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2023, 11, 2, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('7cf3e40577314028b68d7ebcefa85bfd')

    csab911bJob_vol = []
    csab911bJob_volMnt = []
    csab911bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csab911bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csab911bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csab911bJob_env.extend([getICISConfigMap('icis-oder-baseinfo-batch-mng-configmap'), getICISSecret('icis-oder-baseinfo-batch-mng-secret'), getICISConfigMap('icis-oder-baseinfo-batch-configmap'), getICISSecret('icis-oder-baseinfo-batch-secret')])
    csab911bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csab911bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'e17b05be8bb04bad9afb83a2feee185d',
        'volumes': csab911bJob_vol,
        'volume_mounts': csab911bJob_volMnt,
        'env_from':csab911bJob_env,
        'task_id':'csab911bJob',
        'image':'/icis/icis-oder-baseinfo-batch:0.7.1.4',
        'arguments':["--job.name=csab911bJob", "date="+str(datetime.now()), "inWkDate=202411", "inFlag="],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('7cf3e40577314028b68d7ebcefa85bfd')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csab911bJob,
        Complete
    ]) 

    # authCheck >> csab911bJob >> Complete
    workflow








